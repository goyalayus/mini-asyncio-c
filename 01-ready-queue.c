/*
 * 01-ready-queue.c
 *
 * STAGE 1: separate I/O readiness from callback execution with an explicit
 * FIFO ready queue and call_soon().
 *
 * The original mini-epoll-server.c dispatches handlers directly inside the
 * epoll event batch.  This version has two distinct phases:
 *
 *     epoll_wait() -> translate readiness into queued callbacks
 *                  -> run a snapshot of the ready queue
 *
 * A callback queued by another callback waits for the next loop iteration.
 * This is the first scheduler abstraction used by asyncio's event loop.
 *
 * The server is otherwise still the same from-scratch, single-threaded Linux
 * server using raw OS primitives (no libraries, no frameworks):
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
 *   cc -Wall -Wextra -Wpedantic -o mini-ready 01-ready-queue.c
 *   ./mini-ready [num_workers]     (use 1 while studying this stage)
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

    /* Number of queued callbacks that still point at this connection.
     * close_conn() may close the fd immediately, but the struct is freed only
     * after every queued callback has released this reference. */
    int      ready_refs;
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

/* -------- explicit FIFO ready queue ------------------------------------- */

typedef void (*ready_callback)(void *arg);

typedef struct {
    ready_callback callback;
    void          *arg;
    connection    *conn_ref;  /* keeps a connection struct alive while queued */
} ready_handle;

static ready_handle *ready_items;
static size_t        ready_cap;
static size_t        ready_head;
static size_t        ready_len;

static void
ready_grow(void)
{
    size_t new_cap = ready_cap ? ready_cap * 2 : 128;
    ready_handle *new_items = malloc(new_cap * sizeof(*new_items));

    if (!new_items) {
        perror("malloc ready queue");
        exit(1);
    }

    for (size_t i = 0; i < ready_len; i++) {
        new_items[i] = ready_items[(ready_head + i) % ready_cap];
    }

    free(ready_items);
    ready_items = new_items;
    ready_cap   = new_cap;
    ready_head  = 0;
}

/* Schedule a generic callback to run soon, never inline. */
static void
call_soon(ready_callback callback, void *arg)
{
    if (ready_len == ready_cap) ready_grow();

    size_t tail = (ready_head + ready_len) % ready_cap;
    ready_items[tail] = (ready_handle) { callback, arg, NULL };
    ready_len++;
}

/* Same operation for callbacks whose argument is a connection.  The extra
 * reference prevents a close in an earlier callback from creating a dangling
 * pointer in a later queued callback. */
static void
call_soon_conn(ready_callback callback, connection *c)
{
    if (ready_len == ready_cap) ready_grow();

    size_t tail = (ready_head + ready_len) % ready_cap;
    c->ready_refs++;
    ready_items[tail] = (ready_handle) { callback, c, c };
    ready_len++;
}

static ready_handle
ready_pop(void)
{
    ready_handle handle = ready_items[ready_head];
    ready_head = (ready_head + 1) % ready_cap;
    ready_len--;
    return handle;
}

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
static void dispatch_accept(void *arg);
static void dispatch_read(void *arg);
static void dispatch_write(void *arg);
static void dispatch_error(void *arg);

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

/* Free closed connections only when no ready-queue handle still refers to
 * them.  Entries that are still referenced remain on the dead list for the
 * next loop iteration. */
static void
free_dead_connections(void)
{
    int keep = 0;

    for (int i = 0; i < dead_n; i++) {
        connection *c = dead[i];
        if (c->ready_refs == 0) {
            free(c);
        } else {
            dead[keep++] = c;
        }
    }
    dead_n = keep;
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

/* The ready queue stores one generic callback signature.  These small
 * adapters preserve the existing typed handlers while keeping all liveness
 * checks at the point where queued work actually executes. */
static void
dispatch_accept(void *arg)
{
    handler_accept((int) (intptr_t) arg);
}

static void
dispatch_read(void *arg)
{
    connection *c = arg;
    if (c->fd == -1 || !c->rev.active || !c->rev.handler) return;
    c->rev.handler(c);
}

static void
dispatch_write(void *arg)
{
    connection *c = arg;
    if (c->fd == -1 || !c->wev.active || !c->wev.handler) return;
    c->wev.handler(c);
}

static void
dispatch_error(void *arg)
{
    connection *c = arg;
    if (c->fd != -1) close_conn(c);
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
        call_soon_conn(dispatch_read, c);
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
        /* Never sleep in epoll while callbacks are already waiting to run.
         * This matters when a callback used call_soon() during the previous
         * iteration: those newly queued callbacks deliberately wait one tick,
         * but they must not wait for unrelated future network activity. */
        int timeout = ready_len ? 0 : -1;
        int n = epoll_wait(epfd, events, MAX_EVENTS, timeout);

        if (n < 0) {
            if (errno == EINTR) n = 0;
            else { perror("epoll_wait"); exit(1); }
        }

        for (int i = 0; i < n; i++) {
            struct epoll_event *ev = &events[i];

            /* PHASE 1: translate kernel readiness into scheduled callbacks.
             * No application handler runs inside this epoll-event loop. */
            if (ev->data.ptr == (void *) (intptr_t) lsfd) {
                call_soon(dispatch_accept, (void *) (intptr_t) lsfd);
                continue;
            }

            connection *c = ev->data.ptr;
            if (c->fd == -1) continue;

            fprintf(stderr, "loop: fd %d events=0x%x\n", c->fd, ev->events);

            if (ev->events & EPOLLIN) {
                call_soon_conn(dispatch_read, c);
            }

            if (ev->events & EPOLLOUT) {
                call_soon_conn(dispatch_write, c);
            }

            if (ev->events & (EPOLLHUP | EPOLLERR)) {
                call_soon_conn(dispatch_error, c);
            }
        }

        /* PHASE 2: execute only the callbacks that were ready at the start of
         * this phase.  call_soon() from inside one of these callbacks appends
         * work for the next event-loop iteration, matching asyncio's fairness
         * rule and preventing recursive callback chains. */
        size_t ntodo = ready_len;
        for (size_t i = 0; i < ntodo; i++) {
            ready_handle handle = ready_pop();
            handle.callback(handle.arg);

            if (handle.conn_ref) {
                handle.conn_ref->ready_refs--;
            }
        }

        free_dead_connections();
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
