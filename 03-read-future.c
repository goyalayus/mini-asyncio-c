#define _POSIX_C_SOURCE 200809L

/*
 * 03-read-future.c
 *
 * STAGE 3: represent one exact-length socket read as a Future.
 *
 * This version keeps the two stage-1 phases:
 *
 *     epoll_wait() -> translate readiness into queued callbacks
 *                  -> run a snapshot of the ready queue
 *
 * It keeps the ready queue and timers, then inserts a Future between epoll
 * readiness and the application-level request handler:
 *
 *     recv_exact() -> pending Future -> EPOLLIN retries read()
 *                                    -> Future finishes at 100 bytes
 *                                    -> request callback enters ready queue
 *
 * The write side remains callback-driven in this stage.  Only the read side
 * changes, so the Future's responsibility is visible in a focused diff.
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
 *   - while a reply is partially written (wev armed), EPOLLIN stays armed;
 *     the next recv_exact() immediately consumes any bytes already buffered
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
 *   cc -std=c11 -Wall -Wextra -Wpedantic -Werror \
 *      -o mini-read-future 03-read-future.c
 *   ./mini-read-future [num_workers]     (use 1 while studying this stage)
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
#include <time.h>
#include <limits.h>
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

typedef void (*ready_callback)(void *arg);

typedef enum {
    FUTURE_PENDING,
    FUTURE_FINISHED,
    FUTURE_FAILED
} future_state;

typedef struct future_s future;
typedef void (*future_callback)(future *f, void *arg);

struct future_s {
    future_state    state;
    ssize_t         result;
    int             error;
    future_callback done_callback;
    void           *done_arg;
    connection     *owner;
    int             callback_scheduled;
};

typedef struct {
    char    buffer[REQUEST_SIZE];
    size_t  wanted;
    size_t  received;
    future  completion;
} recv_operation;

typedef struct {
    ready_callback callback;
    void          *arg;
    connection    *conn_ref;
} ready_handle;

typedef struct {
    uint64_t       when_ms;
    ready_callback callback;
    void          *arg;
} timer_handle;

/* Both containers are deliberately explicit in this file.  ReadyQueue is a
 * circular FIFO; TimerQueue is a binary min-heap whose root is the earliest
 * deadline. */
typedef struct {
    ready_handle *data;
    size_t        capacity;
    size_t        head;
    size_t        size;
} ReadyQueue;

typedef struct {
    timer_handle *data;
    size_t        capacity;
    size_t        size;
} TimerQueue;

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

    /* One asynchronous exact-length read.  epoll only advances this operation;
     * the embedded Future tells the application when it has completed. */
    recv_operation recv;

    /* send buffer for the reply (nginx: a chain of buffers, c->send_request;
     * we use one small char array) */
    char     buf[64];
    int      buf_pos;                 /* bytes already written out */
    int      buf_len;                 /* total reply length          */

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

static ReadyQueue ready_queue;

static int
ReadyQueue_grow(ReadyQueue *q)
{
    size_t new_capacity = q->capacity ? q->capacity * 2 : 128;
    ready_handle *new_data = malloc(new_capacity * sizeof(*new_data));

    if (!new_data) return 0;

    for (size_t i = 0; i < q->size; i++) {
        new_data[i] = q->data[(q->head + i) % q->capacity];
    }

    free(q->data);
    q->data = new_data;
    q->capacity = new_capacity;
    q->head = 0;
    return 1;
}

static ready_handle *
ReadyQueue_push(ReadyQueue *q, ready_handle value)
{
    if (q->size == q->capacity && !ReadyQueue_grow(q)) return NULL;

    size_t tail = (q->head + q->size) % q->capacity;
    q->data[tail] = value;
    q->size++;
    return &q->data[tail];
}

static ready_handle
ReadyQueue_pull(ReadyQueue *q)
{
    ready_handle value = q->data[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->size--;
    return value;
}

static size_t
ReadyQueue_size(const ReadyQueue *q)
{
    return q->size;
}

static int
ReadyQueue_is_empty(const ReadyQueue *q)
{
    return q->size == 0;
}

/* Schedule a callback while optionally keeping its connection argument alive. */
static void
call_soon_owned(ready_callback callback, void *arg, connection *owner)
{
    ready_handle handle = { callback, arg, owner };
    if (!ReadyQueue_push(&ready_queue, handle)) {
        perror("grow ready queue");
        exit(1);
    }
    if (owner) owner->ready_refs++;
}

/* Schedule a generic callback to run soon, never inline. */
static void
call_soon(ready_callback callback, void *arg)
{
    call_soon_owned(callback, arg, NULL);
}

/* Same operation for callbacks whose argument is a connection.  The extra
 * reference prevents a close in an earlier callback from creating a dangling
 * pointer in a later queued callback. */
static void
call_soon_conn(ready_callback callback, connection *c)
{
    call_soon_owned(callback, c, c);
}

/* -------- one-shot Future ----------------------------------------------- */

static void
future_run_done_callback(void *arg)
{
    future *f = arg;
    f->callback_scheduled = 0;
    f->done_callback(f, f->done_arg);
}

static void
future_schedule_done_callback(future *f)
{
    if (!f->done_callback || f->callback_scheduled) return;
    f->callback_scheduled = 1;
    call_soon_owned(future_run_done_callback, f, f->owner);
}

static void
future_init(future *f, connection *owner)
{
    *f = (future) { .state = FUTURE_PENDING, .owner = owner };
}

static void
future_add_done_callback(future *f, future_callback callback, void *arg)
{
    f->done_callback = callback;
    f->done_arg = arg;
    if (f->state != FUTURE_PENDING) future_schedule_done_callback(f);
}

static void
future_set_result(future *f, ssize_t result)
{
    if (f->state != FUTURE_PENDING) return;
    f->state = FUTURE_FINISHED;
    f->result = result;
    future_schedule_done_callback(f);
}

static void
future_set_error(future *f, int error)
{
    if (f->state != FUTURE_PENDING) return;
    f->state = FUTURE_FAILED;
    f->error = error;
    future_schedule_done_callback(f);
}

/* -------- one-shot timer min-heap --------------------------------------- */

static TimerQueue timer_queue;

/* Increase timer heap capacity when it becomes full. */
static int
TimerQueue_grow(TimerQueue *q)
{
    size_t new_capacity = q->capacity ? q->capacity * 2 : 64;
    timer_handle *new_data = realloc(q->data,
                                     new_capacity * sizeof(*new_data));
    if (!new_data) return 0;

    q->data = new_data;
    q->capacity = new_capacity;
    return 1;
}

/* Insert a timer into the min-heap ordered by earliest expiry time. */
static timer_handle *
TimerQueue_push(TimerQueue *q, timer_handle value)
{
    if (q->size == q->capacity && !TimerQueue_grow(q)) return NULL;

    size_t child = q->size++;
    while (child > 0) {
        size_t parent = (child - 1) / 2;
        if (q->data[parent].when_ms <= value.when_ms) break;
        q->data[child] = q->data[parent];
        child = parent;
    }
    q->data[child] = value;
    return &q->data[child];
}

/* Return the timer that expires next without removing it. */
static const timer_handle *
TimerQueue_top(const TimerQueue *q)
{
    return &q->data[0];
}

/* Remove and return the timer with the earliest expiry time from the heap. */
static timer_handle
TimerQueue_pull(TimerQueue *q)
{
    timer_handle earliest = q->data[0];
    timer_handle last = q->data[--q->size];

    if (q->size == 0) return earliest;

    size_t parent = 0;
    for (;;) {
        size_t left = parent * 2 + 1;
        if (left >= q->size) break;

        size_t right = left + 1;
        size_t earlier = left;
        if (right < q->size
            && q->data[right].when_ms < q->data[left].when_ms)
        {
            earlier = right;
        }

        if (last.when_ms <= q->data[earlier].when_ms) break;
        q->data[parent] = q->data[earlier];
        parent = earlier;
    }
    q->data[parent] = last;
    return earliest;
}

/* Return whether the timer heap contains no timers. */
static int
TimerQueue_is_empty(const TimerQueue *q)
{
    return q->size == 0;
}

/* Return the current monotonic-clock time in milliseconds. */
static uint64_t
monotonic_ms(void)
{
    struct timespec now;

    if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) {
        perror("clock_gettime");
        exit(1);
    }

    return (uint64_t) now.tv_sec * 1000
         + (uint64_t) now.tv_nsec / 1000000;
}

/* Add a timer with an expiry timestamp into the timer priority queue. */
static void
call_later(uint64_t delay_ms, ready_callback callback, void *arg)
{
    uint64_t now = monotonic_ms();
    uint64_t when = delay_ms > UINT64_MAX - now
                  ? UINT64_MAX
                  : now + delay_ms;

    timer_handle timer = { when, callback, arg };
    if (!TimerQueue_push(&timer_queue, timer)) {
        perror("grow timer queue");
        exit(1);
    }
}

/* Return how long epoll_wait should sleep until the next timer expires. */
static int
next_poll_timeout(void)
{
    if (!ReadyQueue_is_empty(&ready_queue)) return 0;
    if (TimerQueue_is_empty(&timer_queue)) return -1;

    uint64_t now = monotonic_ms();
    uint64_t when = TimerQueue_top(&timer_queue)->when_ms;

    if (when <= now) return 0;

    uint64_t delay = when - now;
    return delay > INT_MAX ? INT_MAX : (int) delay;
}

/* Move timers whose expiry time has passed into the ready callback queue. */
static void
queue_expired_timers(void)
{
    uint64_t now = monotonic_ms();

    while (!TimerQueue_is_empty(&timer_queue)
           && TimerQueue_top(&timer_queue)->when_ms <= now)
    {
        timer_handle timer = TimerQueue_pull(&timer_queue);
        call_soon(timer.callback, timer.arg);
    }
}

typedef struct {
    uint64_t requested_delay_ms;
} demo_timer_arg;

static uint64_t demo_timer_started_ms;
static demo_timer_arg demo_timers[] = {
    { 1000 },  /* deliberately inserted first, despite being the latest */
    {  250 },
    {  600 }
};

/* Print when a demonstration timer actually fires. */
static void
demo_timer(void *arg)
{
    demo_timer_arg *timer = arg;
    printf("timer %llu ms fired after %llu ms\n",
           (unsigned long long) timer->requested_delay_ms,
           (unsigned long long) (monotonic_ms() - demo_timer_started_ms));
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
static void start_next_request(connection *c);
static void request_received(future *f, void *arg);
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
        start_next_request(c);
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

/* Advance the pending recv_operation until it completes or would block. */
static void
handler_read(connection *c)
{
    recv_operation *op = &c->recv;
    if (op->completion.state != FUTURE_PENDING) return;

    while (op->received < op->wanted) {
        ssize_t n = read(c->fd, op->buffer + op->received,
                         op->wanted - op->received);

        if (n > 0) {
            op->received += (size_t) n;
            continue;
        }
        if (n == 0) {
            future_set_error(&op->completion, ECONNRESET);
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;

        future_set_error(&op->completion, errno);
        return;
    }

    future_set_result(&op->completion, (ssize_t) op->received);
}

/* Begin one exact-length read and return its eventual completion object. */
static future *
recv_exact(connection *c, size_t length)
{
    recv_operation *op = &c->recv;
    op->wanted = length;
    op->received = 0;
    future_init(&op->completion, c);

    /* Always try immediately.  This is essential under EPOLLET when bytes
     * arrived while no receive operation was pending. */
    handler_read(c);
    return &op->completion;
}

/* Consume a completed read Future at the application layer. */
static void
request_received(future *f, void *arg)
{
    connection *c = arg;
    if (c->fd == -1) return;

    if (f->state == FUTURE_FAILED) {
        if (c->recv.received > 0) {
            printf("incomplete request: received %zu/%zu bytes — closing fd %d\n",
                   c->recv.received, c->recv.wanted, c->fd);
        } else if (f->error != ECONNRESET) {
            errno = f->error;
            perror("read");
        }
        close_conn(c);
        return;
    }

    printf("request %d complete on fd %d — replying\n", c->n + 1, c->fd);
    c->n++;
    c->buf_len = snprintf(c->buf, sizeof(c->buf), "hi %d\n", c->n);
    c->buf_pos = 0;
    write_flow(c);
}

/* Start the next request and arrange application work for its completion. */
static void
start_next_request(connection *c)
{
    future *f = recv_exact(c, REQUEST_SIZE);
    future_add_done_callback(f, request_received, c);
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
            /* Outbox full: arm EPOLLOUT without dropping EPOLLIN. Any client
             * bytes remain buffered until the next recv_exact() starts. */
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

    /* recv_exact() tries read() immediately, so it catches bytes or EOF that
     * arrived while the reply was being flushed—even if an EPOLLET edge was
     * already consumed while no receive operation was pending. */
    start_next_request(c);
}

/* -------- the ONE event loop ------------------------------------------------- */

static void
event_loop_forever(int lsfd)
{
    struct epoll_event events[MAX_EVENTS];

    for (;;) {
        /* Never sleep while callbacks are ready.  Otherwise sleep only until
         * the nearest timer deadline; -1 still means no work and no timers. */
        int timeout = next_poll_timeout();
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

        /* PHASE 2: deadlines that passed while epoll_wait() slept become
         * ordinary FIFO callbacks.  We still do not execute them here. */
        queue_expired_timers();

        /* PHASE 3: execute only the callbacks that were ready at the start of
         * this phase.  call_soon() from inside one of these callbacks appends
         * work for the next event-loop iteration, matching asyncio's fairness
         * rule and preventing recursive callback chains. */
        size_t ntodo = (size_t) ReadyQueue_size(&ready_queue);
        for (size_t i = 0; i < ntodo; i++) {
            ready_handle handle = ReadyQueue_pull(&ready_queue);
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

    /* Insert out of deadline order so the output visibly proves that the
     * min-heap, rather than insertion order, chooses the next callback. */
    demo_timer_started_ms = monotonic_ms();
    for (size_t i = 0; i < sizeof(demo_timers) / sizeof(demo_timers[0]); i++) {
        call_later(demo_timers[i].requested_delay_ms,
                   demo_timer, &demo_timers[i]);
    }

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
