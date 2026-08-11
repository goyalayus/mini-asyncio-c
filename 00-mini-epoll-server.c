/*
 * mini-epoll-server.c
 *
 * A from-scratch, single-threaded event loop server on Linux, using ONLY the
 * raw OS primitives (no libraries, no frameworks):
 *
 *     socket() → bind() → listen() → epoll_create() → epoll_ctl() → epoll_wait()
 *                          → accept() → read() → write() → close()
 *
 * It mimics the nginx worker pattern so you can map what you learn to the
 * nginx source later:
 *
 *   - ONE thread, ONE event loop (for(;;){ epoll_wait(); dispatch; })
 *   - all sockets non-blocking          (O_NONBLOCK)
 *   - edge-triggered notifications      (EPOLLET)  <- the "drain-to-EAGAIN" contract
 *   - TWO event objects per connection: rev (read) and wev (write),
 *     each with its OWN handler function pointer + active flag
 *     (this is nginx's c->read / c->write design)
 *   - the epoll registration is the UNION of both interests, rebuilt by
 *     epoll_sync() whenever either toggles:
 *         rev=1, wev=0  →  EPOLLIN
 *         rev=1, wev=1  →  EPOLLIN | EPOLLOUT  (a reply is being flushed)
 *   - TCP is full duplex: read and write readiness are INDEPENDENT. One
 *     epoll_wait() wakeup can report both, and the loop honors both —
 *     read handler first, then write — for the same connection
 *   - while a reply is partially written (wev armed), reads STAY ON:
 *     new client bytes set req_pending and are processed as soon as the
 *     reply is out
 *   - keep-alive: the connection stays registered on EPOLLIN between
 *     requests, so a client that keeps talking is read immediately
 *
 * The whole nginx architecture, in miniature:
 *   one MASTER process forks N WORKER processes ("worker_processes N")
 *   - every worker has its OWN copy of memory (fork = copy-on-write)
 *   - every worker has its OWN epoll instance + its OWN event loop
 *   - every worker binds its OWN listening socket with SO_REUSEPORT,
 *     so the KERNEL load-balances new connections across workers
 *     (modern nginx "reuseport" mode - no user-space locks needed)
 *   - if a worker dies, the master respawns it  (crash isolation!)
 *
 * This is HOW nginx uses multiple CPU cores: not threads, but several
 * single-threaded loops, each pinned to its own core's worth of work.
 *
 * Build & run:
 *   cc -o mini-server mini-epoll-server.c
 *   ./mini-server [num_workers]     (default: one per CPU core)
 *
 * Test the fixed-length protocol (one request == exactly 100 bytes;
 * reply arrives only when all 100 have accumulated):
 *   (head -c 30  /dev/zero | tr '\0' 'a'; sleep 0.3;
 *    head -c 70  /dev/zero | tr '\0' 'a') | nc 127.0.0.1 8080   -> "hi 1"
 *   send 30 bytes  -> no reply yet
 *   send 70 bytes  -> reply "hi 1"
 *   send 100 bytes -> reply "hi 2"  (on a fresh/keep-alive connection)
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/wait.h>

#define PORT         8080
#define MAX_EVENTS   64

/* APPLICATION-LEVEL protocol: one request = exactly this many bytes.
 * Nothing more — no delimiters, no headers. This constant is what makes
 * a request "complete" (shown as APPLICATION below). */
#define REQUEST_SIZE 100

/* -------- the event object: our ngx_event_t in miniature -------- */

typedef struct connection_s connection;

typedef struct {
    void (*handler)(connection *c);   /* run when THIS event fires */
    int   active;                     /* is the kernel watching for it? */
} event;

/* -------- the "connection" object: our ngx_connection_t in miniature -------- */

struct connection_s {
    int      fd;                      /* which socket this record refers to */

    /* TWO event objects, exactly like nginx's c->read / c->write.
     * The kernel mask is the UNION built by epoll_sync():
     *
     *     rev=1, wev=0  →  EPOLLIN
     *     rev=0, wev=1  →  EPOLLOUT
     *     rev=1, wev=1  →  EPOLLIN | EPOLLOUT
     *
     * TCP is full duplex: read readiness and write readiness are
     * INDEPENDENT, and the kernel can report both at once. */
    event    rev;                     /* read event  — want EPOLLIN  */
    event    wev;                     /* write event — want EPOLLOUT */
    int      registered;              /* fd already ADD'ed in epoll? (ADD vs MOD) */

    /*  persistent INPUT state for the fixed-length protocol.
     *  epoll / TCP tells us      : "bytes are available"
     *  c->request_pos tells us   : how much of the current 100-byte
     *                              request has actually arrived
     *  REQUEST_SIZE == 100 defines: when the application considers one
     *                              request complete
     *  (calloc() made request_pos 0 at accept time.) */
    char     request_buf[REQUEST_SIZE];
    int      request_pos;

    /* send buffer for the reply (nginx: a chain of buffers, c->send_request;
     * we use one small char array) */
    char     buf[64];
    int      buf_pos;                 /* bytes already written out */
    int      buf_len;                 /* total reply length          */

    int      req_pending;             /* input arrived while a reply was still
                                       * being flushed: do NOT overwrite buf,
                                       * leave the bytes in the kernel buffer */
    int      n;                       /* request counter, for showing "hi 2" */
};

static int      epfd;               /* our single epoll instance */

/* DEFERRED FREE — the only global machinery this file needs.
 *
 * A single epoll_event can carry BOTH EPOLLIN and EPOLLOUT for the same
 * fd.  We dispatch the read handler first; it may close the connection.
 * To evaluate whether the write half should still run, we must be able
 * to look at the connection after the read handler returned — so the
 * struct must NOT be freed inside close_conn().
 *
 * close_conn() parks dying connections here (marking fd = -1 first, the
 * same marker nginx uses); the event loop skips any event whose c->fd
 * is -1, and frees everything only AFTER the whole epoll_wait batch has
 * been processed.  That is the whole trick: the struct outlives the
 * batch, but never outlives it long.  (Production nginx goes further
 * with pools + an "instance" bit; here the deferred free alone is
 * enough to safely demonstrate simultaneous IN|OUT dispatch.) */
static connection *dead[4096];
static int         dead_n;

/* -------- tiny helpers --------------------------------------------------- */

static void
set_nonblocking(int fd)
{
    int fl = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/* Build a listening socket, NON-BLOCKING, ready for epoll interest.
 * SO_REUSEPORT: allows MANY processes to bind the SAME port. The kernel
 * then distributes incoming connections among them by a hash — no locks. */
static int
create_listener(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int on = 1;

    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port        = htons(PORT);

    set_nonblocking(fd);
    if (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0) {
        perror("bind"); exit(1);
    }
    if (listen(fd, 128) < 0) {
        perror("listen"); exit(1);
    }
    return fd;
}

/* Register interest in an fd inside the epoll instance.
 * events = EPOLLIN   -> "tell me when I can read"
 *          EPOLLOUT  -> "tell me when I can write" */
static void
epoll_watch(int fd, uint32_t events, void *data)
{
    struct epoll_event ev;
    ev.events   = events | EPOLLET;      /* edge-triggered, always */
    ev.data.ptr = data;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

/* THE bridge between our two events and the kernel's ONE mask.
 *
 * The kernel does not know about handlers. It only knows a single
 * interest mask per fd. So whenever rev or wev toggles, we recompute
 *   EPOLLIN  (if rev.active)
 *   EPOLLOUT (if wev.active)
 * and push the whole thing with one epoll_ctl call.
 * This is exactly what nginx does with c->read->active / c->write->active. */
static void
epoll_sync(connection *c)
{
    struct epoll_event ev;
    uint32_t m = 0;

    if (c->rev.active) m |= EPOLLIN;
    if (c->wev.active) m |= EPOLLOUT;

    ev.events   = m | EPOLLET;
    ev.data.ptr = c;

    epoll_ctl(epfd, c->registered ? EPOLL_CTL_MOD : EPOLL_CTL_ADD,
              c->fd, &ev);
    c->registered = 1;
}

/* forward decls used below */
static void handler_read(connection *c);
static void write_flow(connection *c);

static void
close_conn(connection *c)
{
    if (!c || c->fd == -1) return;               /* already dead */
    printf("close fd %d\n", c->fd);
    epoll_ctl(epfd, EPOLL_CTL_DEL, c->fd, NULL);  /* kernel forgets it */
    close(c->fd);
    c->fd = -1;                                  /* marker: this conn is gone */
    dead[dead_n++] = c;                          /* freed after the batch */
}

/* Accept everyone currently waiting. Because we use EPOLLET we must drain
 * the accept queue until "no more right now" (EAGAIN) - the same discipline
 * we studied for reads. */
static void
handler_accept(int lsfd)
{
    for (;;) {
        int fd = accept(lsfd, NULL, NULL);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;  /* drained */
            perror("accept");
            break;
        }

        set_nonblocking(fd);

        connection *c = calloc(1, sizeof(connection));
        c->fd = fd;

        /* initial stage: only want to READ (nginx: rev->handler = wait_for_request) */
        c->rev.handler = handler_read;
        c->rev.active  = 1;
        c->wev.active  = 0;              /* NOT watching for writes yet */

        printf("worker %d: accept fd %d\n", (int) getpid(), fd);
        epoll_sync(c);                   /* ADD: mask = EPOLLIN */
    }
}

/* called by the loop when a socket is READABLE ("bytes are available").
 *
 * THE KEY LESSON of this file:
 *   EAGAIN does NOT mean "a request is complete".
 *   It only means "no more bytes are available right now".
 *   Request completion is decided by the APPLICATION: request_pos must
 *   reach REQUEST_SIZE (100).  TCP may deliver those 100 bytes in any
 *   chunking — 30+20+50, 1+99, 100 at once — we just accumulate. */
static void
handler_read(connection *c)
{
    /* OUTPUT-BUFFER GUARD: while a reply is still being flushed
     * (buf_pos < buf_len), we must NOT overwrite c->buf with a new
     * response.  The freshly arrived bytes are safe — they sit in the
     * kernel's receive buffer.  Mark them as waiting; write_flow()
     * processes them the moment the current reply is fully out. */
    if (c->buf_pos < c->buf_len) {
        c->req_pending = 1;
        return;
    }

    for (;;) {
        /* Read ONLY up to the end of the current request: at most
         * (REQUEST_SIZE - request_pos) bytes per read().  If a big burst
         * carries several requests, the excess stays in the kernel buffer
         * and this loop comes back for it — never beyond the boundary. */
        while (c->request_pos < REQUEST_SIZE) {
            ssize_t n = read(c->fd,
                             c->request_buf + c->request_pos,
                             REQUEST_SIZE - c->request_pos);

            if (n > 0) { c->request_pos += (int) n; continue; }
            if (n == 0) {
                /* peer permanently stopped sending.  We are still inside
                 * the while (request_pos < REQUEST_SIZE) loop, so the
                 * request is NECESSARILY incomplete (if it had reached
                 * 100 we would have left the loop already).  Log and
                 * drop the connection immediately. */
                if (c->request_pos > 0) {
                    printf("incomplete request: received %d/%d bytes — closing fd %d\n",
                           c->request_pos, REQUEST_SIZE, c->fd);
                }
                close_conn(c);
                return;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* drained — request possibly partial. This EAGAIN is NOT
                 * "request complete"; the kernel will fire a fresh
                 * EPOLLIN edge when more bytes arrive. */
                return;
            }
            perror("read"); close_conn(c); return;
        }

        /* request_pos == REQUEST_SIZE: application says "request complete". */
        printf("request %d complete on fd %d — replying\n", c->n + 1, c->fd);

        c->request_pos = 0;          /* reset for the NEXT 100-byte request */

        c->n++;
        c->buf_len = snprintf(c->buf, sizeof(c->buf), "hi %d\n", c->n);
        c->buf_pos = 0;

        write_flow(c);

        /* if the reply went out but needs more EPOLLOUT round-trips,
         * stop here — rev stays registered, req_pending guards reads */
        if (c->buf_pos < c->buf_len)
            return;

        /* reply fully flushed: keep draining for more complete requests
         * (EPOLLET discipline: don't stop while bytes are available).
         * A client that closes after its requests are served is caught
         * by the read()==0 branch above on the next pass. */
    }
}

/* Try to send the pending reply. On EAGAIN we arm EPOLLOUT but keep
 * EPOLLIN too — reads and writes are independent, so the mask becomes
 * EPOLLIN | EPOLLOUT.  When everything is out, only EPOLLOUT is dropped.
 *
 * This function is ALSO the wev.handler: the write event calls it directly
 * when the kernel says the outbox has room again (no wrapper needed). */
static void
write_flow(connection *c)
{
    while (c->buf_pos < c->buf_len) {
        ssize_t n = write(c->fd, c->buf + c->buf_pos, c->buf_len - c->buf_pos);
        if (n > 0) { c->buf_pos += (int) n; continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            /* outbox full! Do NOT busy-wait and do NOT pause reads:
             * arm EPOLLOUT and keep EPOLLIN. The kernel now watches
             * BOTH, and the loop will wake us when the socket can take
             * more bytes — while any new client bytes get noticed via
             * the still-registered read interest. */
            c->wev.handler = write_flow;
            c->wev.active  = 1;            /* rev.active stays 1! */
            epoll_sync(c);                 /* mask: EPOLLIN | EPOLLOUT */
            printf("outbox full on fd %d — arming EPOLLOUT (reads stay on)\n",
                   c->fd);
            return;
        }
        /* real error */
        close_conn(c);
        return;
    }

    /* everything went out — drop ONLY the write interest; reading was
     * never disabled, so nothing to restore. */
    if (c->wev.active) {
        c->wev.active  = 0;
        c->wev.handler = NULL;
        epoll_sync(c);                     /* mask back to: EPOLLIN */
    }

    /* if input arrived while the reply was being flushed, it is marked
     * req_pending but still sits in the KERNEL buffer. Process it NOW,
     * directly: under EPOLLET that IN edge may already have been
     * consumed by the earlier handler_read, so we must NOT wait for
     * another one. (And we return right after: handler_read may have
     * closed the connection.) */
    if (c->req_pending) {
        c->req_pending = 0;
        handler_read(c);
        return;
    }

    /* EOF needs no state here: a peer that closes after its requests are
     * served is caught by the read()==0 branch in handler_read. */
}

/* -------- the ONE event loop ------------------------------------------------- */

static void
event_loop_forever(int lsfd)
{
    struct epoll_event events[MAX_EVENTS];

    for (;;) {
        int n = epoll_wait(epfd, events, MAX_EVENTS, -1);  /* sleep; kernel wakes me */

        for (int i = 0; i < n; i++) {
            struct epoll_event *ev = &events[i];

            /* the listening socket became readable -> someone wants to connect */
            if (ev->data.ptr == (void *) (intptr_t) lsfd) {
                handler_accept(lsfd);
                continue;
            }

            connection *c = ev->data.ptr;

            /* STALE-EVENT GUARD: if the SAME batch carried two events
             * for this fd and the first one closed the connection, this
             * entry is garbage. Safe to check: the struct is still
             * allocated — it is parked on the dead list, freed only
             * after the batch. */
            if (c->fd == -1) continue;

            fprintf(stderr, "loop: fd %d events=0x%x\n", c->fd, ev->events);

            /* THE DISPATCH — one wakeup, possibly TWO events.
             *
             * The kernel reports ONE mask per wakeup; EPOLLIN and
             * EPOLLOUT can arrive together for the same fd. We run the
             * read half first, then — if the connection survived — the
             * write half.  Both conditions are honored from ONE returned
             * epoll_event; nothing is discarded with a continue.
             *
             * Between calls we check c->fd == -1: the read handler may
             * have closed the connection (parked on the dead list, not
             * freed), so the write half must not run for it. */
            if (ev->events & EPOLLIN) {
                c->rev.handler(c);       /* read-handler can close c! */
            }

            if (c->fd == -1) continue;   /* read half killed the conn: stop */

            if ((ev->events & EPOLLOUT) && c->wev.active) {
                c->wev.handler(c);       /* finish the stuck reply */
            }

            if (c->fd == -1) continue;   /* write handler closed it: stop */

            if (ev->events & (EPOLLHUP | EPOLLERR)) {
                close_conn(c);
            }
        }

        /* END OF BATCH: only now is it safe to actually release every
         * connection closed during this wakeup.  The structs have been
         * alive all through the batch — that is what made the
         * c->fd == -1 checks above legal. */
        for (int i = 0; i < dead_n; i++) free(dead[i]);
        dead_n = 0;
    }
}

/* One worker process. Everything we studied, per worker:
 *   own epoll instance, own listening socket, own loop, own memory. */
static void
worker_main(void)
{
    setvbuf(stdout, NULL, _IONBF, 0);    /* DEBUG: show output immediately */

    epfd = epoll_create(1);              /* ONE epoll instance, per worker */

    int lsfd = create_listener();    /* already non-blocking inside */

    epoll_watch(lsfd, EPOLLIN, (void *) (intptr_t) lsfd);   /* interest: NEW connections */
    printf("worker pid %d: listening on %d\n", (int) getpid(), PORT);

    event_loop_forever(lsfd);
}

int
main(int argc, char **argv)
{
    int   nworkers, i, status;
    pid_t pid;

    signal(SIGPIPE, SIG_IGN);            /* dying client must not crash us */

    if (argc > 1) {
        nworkers = atoi(argv[1]);
    } else {
        nworkers = (int) sysconf(_SC_NPROCESSORS_ONLN);  /* one per CPU core */
    }
    if (nworkers < 1) nworkers = 1;

    printf("master pid %d: forking %d workers\n", (int) getpid(), nworkers);

    /* the "worker_processes N" directive, hand-rolled */
    for (i = 0; i < nworkers; i++) {
        pid = fork();
        if (pid < 0) { perror("fork"); exit(1); }
        if (pid == 0) { worker_main(); }       /* child: never returns */
    }

    /* the master's job now: babysit. If a worker dies, respawn it.
     * THIS is crash isolation - a dead worker never kills the others. */
    for (;;) {
        pid = waitpid(-1, &status, 0);
        if (pid < 0) { perror("waitpid"); exit(1); }
        printf("master: worker pid %d died (status %d), respawning\n",
               (int) pid, WEXITSTATUS(status));

        pid = fork();
        if (pid < 0) { perror("fork"); exit(1); }
        if (pid == 0) { worker_main(); }
    }
}