# mini-asyncio-c

Learn how Python-style asynchronous execution works by building its machinery
one explicit step at a time in C on Linux.

This is intentionally a fixed-length TCP protocol, not HTTP. Keeping the
application protocol tiny makes the event loop, scheduling, suspension, and
resumption mechanics visible.

## Stages

### `00-mini-epoll-server.c`

The untouched baseline server:

- nonblocking sockets
- edge-triggered `epoll`
- direct read/write callback dispatch
- partial-read and partial-write state
- multiple worker processes

### `01-ready-queue.c`

Separates readiness discovery from callback execution:

```text
epoll_wait -> call_soon(callback, argument) -> FIFO ready queue
                                                   |
                                                   v
                                         execute a queue snapshot
```

This corresponds to CPython asyncio's `_ready` deque, `call_soon()`,
`_process_events()`, and the callback-execution portion of `_run_once()`:

- [`Lib/asyncio/base_events.py`](https://github.com/python/cpython/blob/v3.14.6/Lib/asyncio/base_events.py)
- [`Lib/asyncio/selector_events.py`](https://github.com/python/cpython/blob/v3.14.6/Lib/asyncio/selector_events.py)
- [`Lib/asyncio/events.py`](https://github.com/python/cpython/blob/v3.14.6/Lib/asyncio/events.py)

The additional `ready_refs` counter is manual C lifetime management. Python
normally keeps queued callback arguments alive through object reference
counting.

### `02-timers.c`

Adds delayed, one-shot callback scheduling:

```text
call_later(delay, callback, argument) -> timer min-heap
                                                |
                         nearest deadline becomes epoll timeout
                                                |
                         expired timer enters the ready queue
```

Important properties:

- deadlines use `CLOCK_MONOTONIC`, so wall-clock changes cannot move timers
- the heap root always contains the earliest deadline
- existing ready work forces a zero-millisecond poll
- no ready work and no timers allows an infinite poll
- timer callbacks use the same FIFO execution path as I/O callbacks
- three startup timers, inserted out of order, visibly demonstrate heap order

The ready FIFO and timer heap are both defined directly in `02-timers.c`.
There are no generated types or container dependencies, so the entire stage
can be read and navigated as one self-contained file.

This corresponds to asyncio's `_scheduled` heap, `call_later()`, `call_at()`,
and the timeout/timer portions of `BaseEventLoop._run_once()`.

### `03-read-future.c`

Moves exact-length read completion out of the application handler and into a
one-shot Future embedded in `recv_operation`:

```text
recv_exact(100) -> Future pending -> EPOLLIN retries read()
                                  -> Future finishes at 100 bytes
                                  -> request callback enters ready queue
```

The Future stores either a result or an error and schedules its done callback
through `call_soon_owned()`. The ready handle keeps the connection alive until
that callback has run. Writes remain callback-driven in this checkpoint so the
read-side integration can be studied independently.

### `04-final-async-server.c`

The compact, integrated version. One generic `Task` drives a manually written
coroutine object, and every suspension point yields a `Future`:

```text
client coroutine -> await read_exact(100)
                 -> await sleep(10ms)
                 -> await write("hi N\n")
```

For this one concrete client coroutine, the connection itself is its frame: it
contains the instruction pointer and all state that must survive suspension.
The generic Task only knows how to resume that frame and await the Future it
yields. Separate client Tasks run concurrently on one OS thread. The reactor
uses level-triggered epoll, matching CPython's Linux selector path; the
nonblocking operation is still attempted immediately before waiting again.

### `05-final-async-server.cpp`

Rewrites the integrated server with ordinary C++ classes while keeping the
async machinery explicit and local to this repository:

- `EventLoop` owns epoll, the ready queue, and the timer priority queue
- `Future` stores completion and schedules its waiter
- `Task` resumes a `Coroutine` and connects it to the yielded `Future`
- `Connection` is the coroutine frame and contains the explicit client state

This checkpoint does not use Boost.Asio, C++20 coroutines, or another async
runtime. The `await` result, task stepping, suspension, resumption, readiness
registration, and state machine are all implemented in the file.

## Build and run

The code uses Linux `epoll`. On Linux:

```sh
c++ -std=c++17 -Wall -Wextra -Wpedantic -Werror \
  -o mini-final-cpp 05-final-async-server.cpp
./mini-final-cpp
```

On macOS, build and run it in Docker:

```sh
docker build -t mini-asyncio-c .
docker run --rm -p 8080:8080 mini-asyncio-c
```

Send one 100-byte request:

```sh
head -c 100 /dev/zero | tr '\0' a | nc 127.0.0.1 8080
```

Expected response:

```text
hi 1
```

Without any network activity, stage 2 also prints timers in deadline order,
not insertion order:

```text
timer 250 ms fired after 250 ms
timer 600 ms fired after 600 ms
timer 1000 ms fired after 1000 ms
```

Run the parallel connection test:

```sh
./load-test.sh 200
```

## Learning rule

Each file is a checkpoint. A later stage may copy and extend the preceding
stage, but earlier stages remain unchanged so the new abstraction can be
studied as a focused diff.
