#define _POSIX_C_SOURCE 200809L

/*
 * A small Python-style async runtime in one C file:
 *
 *   epoll -> ready queue -> Future -> Task -> coroutine state machine
 *                         \-> timer queue
 *
 * Each connection runs the equivalent of:
 *
 *   while True:
 *       await read_exact(100)
 *       await sleep(10ms)
 *       await write("hi N\n")
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define PORT 8080
#define REQUEST_SIZE 100
#define MAX_EVENTS 128

typedef struct connection connection;
typedef struct future future;

typedef void (*callback_fn)(void *);

typedef struct {
  callback_fn fn;
  void *arg;
  connection *owner; /* keeps arg alive until the callback runs */
} callback;

typedef enum { FUTURE_PENDING, FUTURE_FINISHED, FUTURE_FAILED } future_state;

struct future {
  future_state state;
  ssize_t result;
  int error;
  callback_fn waiter;
  void *waiter_arg;
};

typedef struct {
  callback *data;
  size_t head, size, capacity;
} ready_queue;

typedef struct {
  uint64_t when_ms;
  future *future;
  connection *owner;
} timer;

typedef struct {
  timer *data;
  size_t size, capacity;
} timer_queue;

typedef future *(*resume_fn)(void *frame);

typedef struct {
  resume_fn resume;
  future *waiting_on;
} task;

typedef enum {
  CLIENT_READ,
  CLIENT_AFTER_READ,
  CLIENT_AFTER_SLEEP,
  CLIENT_AFTER_WRITE
} client_state;

struct connection {
  int fd;
  int references;
  int want_read;
  int want_write;
  int request_number;
  client_state state; /* coroutine instruction pointer */

  char read_buffer[REQUEST_SIZE];
  size_t read_used;
  future operation; /* the one operation currently awaited */

  char write_buffer[64];
  size_t write_used;
  size_t write_length;

  task client_task;
};

static int epoll_fd;
static ready_queue ready;
static timer_queue timers;

static void task_step(void *arg);
static void close_connection(void *arg);
static void call_soon_conn(callback_fn fn, connection *c);
static void future_result(future *f, ssize_t result);

static void die(const char *what) {
  perror(what);
  exit(1);
}

static uint64_t now_ms(void) {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) < 0)
    die("clock_gettime");
  return (uint64_t)ts.tv_sec * 1000 + (uint64_t)ts.tv_nsec / 1000000;
}

static void retain(connection *c) { c->references++; }

static void release(connection *c) {
  if (--c->references == 0)
    free(c);
}

static void ready_push(callback work) {
  if (ready.size == ready.capacity) {
    size_t capacity = ready.capacity ? ready.capacity * 2 : 128;
    callback *data = malloc(capacity * sizeof(*data));
    if (!data)
      die("malloc ready queue");
    for (size_t i = 0; i < ready.size; i++)
      data[i] = ready.data[(ready.head + i) % ready.capacity];
    free(ready.data);
    ready.data = data;
    ready.capacity = capacity;
    ready.head = 0;
  }
  ready.data[(ready.head + ready.size++) % ready.capacity] = work;
}

static callback ready_pull(void) {
  callback work = ready.data[ready.head];
  ready.head = (ready.head + 1) % ready.capacity;
  ready.size--;
  return work;
}

/* Queue a connection callback and hide the lifetime bookkeeping from callers.
 */
static void call_soon_conn(callback_fn fn, connection *c) {
  retain(c);
  ready_push((callback){fn, c, c});
}

static void timer_push(timer value) {
  if (timers.size == timers.capacity) {
    size_t capacity = timers.capacity ? timers.capacity * 2 : 64;
    timer *data = realloc(timers.data, capacity * sizeof(*data));
    if (!data)
      die("realloc timer queue");
    timers.data = data;
    timers.capacity = capacity;
  }

  size_t child = timers.size++;
  while (child) {
    size_t parent = (child - 1) / 2;
    if (timers.data[parent].when_ms <= value.when_ms)
      break;
    timers.data[child] = timers.data[parent];
    child = parent;
  }
  timers.data[child] = value;
}

static timer timer_pull(void) {
  timer first = timers.data[0];
  timer last = timers.data[--timers.size];
  size_t parent = 0;

  while (parent * 2 + 1 < timers.size) {
    size_t child = parent * 2 + 1;
    if (child + 1 < timers.size &&
        timers.data[child + 1].when_ms < timers.data[child].when_ms)
      child++;
    if (last.when_ms <= timers.data[child].when_ms)
      break;
    timers.data[parent] = timers.data[child];
    parent = child;
  }
  if (timers.size)
    timers.data[parent] = last;
  return first;
}

static int poll_timeout(void) {
  if (ready.size)
    return 0;
  if (!timers.size)
    return -1;
  uint64_t now = now_ms(), when = timers.data[0].when_ms;
  if (when <= now)
    return 0;
  uint64_t delay = when - now;
  return delay > INT32_MAX ? INT32_MAX : (int)delay;
}

static void expire_timers(void) {
  uint64_t now = now_ms();
  while (timers.size && timers.data[0].when_ms <= now) {
    timer expired = timer_pull();
    future_result(expired.future, 0);
    release(expired.owner);
  }
}

static void future_reset(future *f) { *f = (future){.state = FUTURE_PENDING}; }

static void future_wake_waiter(future *f) {
  if (!f->waiter)
    return;
  callback_fn fn = f->waiter;
  void *arg = f->waiter_arg;
  f->waiter = NULL;
  f->waiter_arg = NULL;
  call_soon_conn(fn, arg);
}

static void future_result(future *f, ssize_t result) {
  if (f->state != FUTURE_PENDING)
    return;
  f->state = FUTURE_FINISHED;
  f->result = result;
  future_wake_waiter(f);
}

static void future_error(future *f, int error) {
  if (f->state != FUTURE_PENDING)
    return;
  f->state = FUTURE_FAILED;
  f->error = error;
  future_wake_waiter(f);
}

static void future_add_waiter(future *f, callback_fn fn, connection *c) {
  f->waiter = fn;
  f->waiter_arg = c;
  if (f->state != FUTURE_PENDING)
    future_wake_waiter(f);
}

static void sync_epoll(connection *c) {
  struct epoll_event event = {0};
  event.events = (c->want_read ? EPOLLIN : 0) | (c->want_write ? EPOLLOUT : 0);
  event.data.ptr = c;
  if (epoll_ctl(epoll_fd, EPOLL_CTL_MOD, c->fd, &event) < 0)
    die("epoll_ctl modify");
}

static void close_connection(void *arg) {
  connection *c = arg;
  if (c->fd == -1)
    return;
  epoll_ctl(epoll_fd, EPOLL_CTL_DEL, c->fd, NULL);
  close(c->fd);
  c->fd = -1;
  if (c->client_task.waiting_on &&
      c->client_task.waiting_on->state == FUTURE_PENDING)
    future_error(c->client_task.waiting_on, ECANCELED);
  release(c); /* release the connection's event-loop ownership */
}

static void advance_read(connection *c) {
  while (c->read_used < REQUEST_SIZE) {
    ssize_t n =
        read(c->fd, c->read_buffer + c->read_used, REQUEST_SIZE - c->read_used);
    if (n > 0) {
      c->read_used += (size_t)n;
      continue;
    }
    if (n == 0) {
      future_error(&c->operation, ECONNRESET);
      break;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
      break;
    future_error(&c->operation, errno);
    break;
  }
  if (c->read_used == REQUEST_SIZE)
    future_result(&c->operation, (ssize_t)c->read_used);
  if (c->operation.state != FUTURE_PENDING) {
    c->want_read = 0;
    sync_epoll(c);
  }
}

static future *read_exact(connection *c) {
  c->read_used = 0;
  future_reset(&c->operation);
  c->want_read = 1;
  sync_epoll(c);
  advance_read(c); /* catches bytes already buffered before this await */
  return &c->operation;
}

static void advance_write(connection *c) {
  while (c->write_used < c->write_length) {
    ssize_t n = write(c->fd, c->write_buffer + c->write_used,
                      c->write_length - c->write_used);
    if (n > 0) {
      c->write_used += (size_t)n;
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
      break;
    future_error(&c->operation, n < 0 ? errno : EPIPE);
    break;
  }
  if (c->write_used == c->write_length)
    future_result(&c->operation, (ssize_t)c->write_used);
  c->want_write = c->operation.state == FUTURE_PENDING;
  sync_epoll(c);
}

static future *write_async(connection *c, const char *data, size_t length) {
  future_reset(&c->operation);
  if (length > sizeof(c->write_buffer)) {
    future_error(&c->operation, EMSGSIZE);
    return &c->operation;
  }
  memcpy(c->write_buffer, data, length);
  c->write_used = 0;
  c->write_length = length;
  c->want_write = 1;
  sync_epoll(c);
  advance_write(c);
  return &c->operation;
}

static future *sleep_async(connection *c, uint64_t delay_ms) {
  future_reset(&c->operation);
  retain(c);
  timer_push((timer){now_ms() + delay_ms, &c->operation, c});
  return &c->operation;
}

static void task_step(void *arg) {
  connection *c = arg;
  future *completed = c->client_task.waiting_on;
  c->client_task.waiting_on = NULL;

  if (completed && completed->state == FUTURE_FAILED) {
    close_connection(c);
    return;
  }

  future *awaitable = c->client_task.resume(c);
  if (awaitable) {
    c->client_task.waiting_on = awaitable;
    future_add_waiter(awaitable, task_step, c);
  }
}

static future *client_resume(void *arg) {
  connection *c = arg;

  for (;;) {
    switch (c->state) {
    case CLIENT_READ:
      c->state = CLIENT_AFTER_READ;
      return read_exact(c);

    case CLIENT_AFTER_READ: {
      c->request_number++;
      c->state = CLIENT_AFTER_SLEEP;
      return sleep_async(c, 10);
    }

    case CLIENT_AFTER_SLEEP: {
      char reply[64];
      int length = snprintf(reply, sizeof(reply), "hi %d\n", c->request_number);
      c->state = CLIENT_AFTER_WRITE;
      return write_async(c, reply, (size_t)length);
    }

    case CLIENT_AFTER_WRITE:
      c->state = CLIENT_READ;
      break;
    }
  }
}

static void dispatch_read(void *arg) {
  connection *c = arg;
  if (c->fd != -1 && c->want_read)
    advance_read(c);
}

static void dispatch_write(void *arg) {
  connection *c = arg;
  if (c->fd != -1 && c->want_write)
    advance_write(c);
}

static int make_nonblocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static int create_listener(void) {
  int fd = socket(AF_INET, SOCK_STREAM, 0), yes = 1;
  if (fd < 0)
    die("socket");
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
  struct sockaddr_in address = {.sin_family = AF_INET,
                                .sin_port = htons(PORT),
                                .sin_addr.s_addr = htonl(INADDR_ANY)};
  if (make_nonblocking(fd) < 0)
    die("nonblocking listener");
  if (bind(fd, (struct sockaddr *)&address, sizeof(address)) < 0)
    die("bind");
  if (listen(fd, 256) < 0)
    die("listen");
  return fd;
}

static void accept_ready(int listener) {
  for (;;) {
    int fd = accept(listener, NULL, NULL);
    if (fd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        return;
      die("accept");
    }
    if (make_nonblocking(fd) < 0) {
      close(fd);
      continue;
    }

    connection *c = calloc(1, sizeof(*c));
    if (!c)
      die("calloc connection");
    c->fd = fd;
    c->references = 1;
    c->state = CLIENT_READ;
    c->client_task.resume = client_resume;

    struct epoll_event event = {.events = EPOLLIN, .data.ptr = c};
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &event) < 0)
      die("epoll_ctl add client");
    c->want_read = 1;
    call_soon_conn(task_step, c);
  }
}

static void run_ready_snapshot(void) {
  size_t count = ready.size;
  while (count--) {
    callback work = ready_pull();
    work.fn(work.arg);
    release(work.owner);
  }
}

static void event_loop(int listener) {
  struct epoll_event events[MAX_EVENTS];
  for (;;) {
    int count = epoll_wait(epoll_fd, events, MAX_EVENTS, poll_timeout());
    if (count < 0) {
      if (errno == EINTR)
        count = 0;
      else
        die("epoll_wait");
    }

    for (int i = 0; i < count; i++) {
      if (events[i].data.ptr == NULL) {
        accept_ready(listener);
        continue;
      }
      connection *c = events[i].data.ptr;
      if (c->fd == -1)
        continue;
      if (events[i].events & EPOLLIN)
        call_soon_conn(dispatch_read, c);
      if (events[i].events & EPOLLOUT)
        call_soon_conn(dispatch_write, c);
      if (events[i].events & (EPOLLERR | EPOLLHUP))
        call_soon_conn(close_connection, c);
    }

    expire_timers();
    run_ready_snapshot();
  }
}

int main(void) {
  signal(SIGPIPE, SIG_IGN);
  epoll_fd = epoll_create1(0);
  if (epoll_fd < 0)
    die("epoll_create1");

  int listener = create_listener();
  struct epoll_event event = {.events = EPOLLIN, .data.ptr = NULL};
  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, listener, &event) < 0)
    die("epoll_ctl listener");

  printf("compact async server listening on :%d\n", PORT);
  event_loop(listener);
}
