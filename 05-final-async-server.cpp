#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

constexpr int PORT = 8080;
constexpr std::size_t REQUEST_SIZE = 100;
constexpr int MAX_EVENTS = 128;

class EventLoop;
class Connection;

[[noreturn]] static void die(const char *what)
{
    std::perror(what);
    std::exit(1);
}

enum class FutureState { Pending, Finished, Failed };

class Future {
public:
    explicit Future(EventLoop& loop) : loop_(loop) {}

    void reset();
    void set_result(ssize_t result);
    void set_error(int error);
    void add_waiter(std::function<void()> waiter);

    FutureState state() const { return state_; }
    ssize_t result() const { return result_; }
    int error() const { return error_; }

private:
    void wake_waiter();

    EventLoop& loop_;
    FutureState state_ = FutureState::Pending;
    ssize_t result_ = 0;
    int error_ = 0;
    std::function<void()> waiter_;
};

enum class CoroutineAction { Await, Done };

struct CoroutineResult {
    CoroutineAction action;
    Future *awaitable;

    static CoroutineResult await(Future& future)
    {
        return { CoroutineAction::Await, &future };
    }

    static CoroutineResult done()
    {
        return { CoroutineAction::Done, nullptr };
    }
};

class Coroutine {
public:
    virtual ~Coroutine() = default;
    virtual CoroutineResult resume(Future *completed) = 0;
};

class Task {
public:
    explicit Task(Coroutine& coroutine) : coroutine_(coroutine) {}

    void step(const std::shared_ptr<Connection>& connection);
    Future *waiting_on() const { return waiting_on_; }

private:
    Coroutine& coroutine_;
    Future *waiting_on_ = nullptr;
};

struct Timer {
    std::chrono::steady_clock::time_point when;
    Future *future;
    std::shared_ptr<Connection> owner;
};

struct EarlierTimer {
    bool operator()(const Timer& left, const Timer& right) const
    {
        return left.when > right.when;
    }
};

class EventLoop {
public:
    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    void run(int listener);
    void call_soon(std::function<void()> callback);
    void call_later(std::chrono::milliseconds delay, Future& future,
                    std::shared_ptr<Connection> owner);
    void sync_epoll(const Connection& connection);
    void remove_connection(int fd);

private:
    void accept_ready(int listener);
    int poll_timeout() const;
    void expire_timers();
    void run_ready_snapshot();

    int epoll_fd_;
    std::deque<std::function<void()>> ready_;
    std::priority_queue<Timer, std::vector<Timer>, EarlierTimer> timers_;
    std::unordered_map<int, std::shared_ptr<Connection>> connections_;
};

enum class ClientState { Read, AfterRead, AfterSleep, AfterWrite };

class Connection final : public Coroutine,
                         public std::enable_shared_from_this<Connection> {
public:
    Connection(EventLoop& loop, int fd)
        : loop_(loop), fd_(fd), operation_(loop), task_(*this)
    {
    }

    ~Connection() override
    {
        if (fd_ != -1) ::close(fd_);
    }

    int fd() const { return fd_; }
    bool wants_read() const { return want_read_; }
    bool wants_write() const { return want_write_; }
    Task& task() { return task_; }

    void close();
    void dispatch_read();
    void dispatch_write();
    CoroutineResult resume(Future *completed) override;

private:
    Future& read_exact();
    Future& sleep_async(std::chrono::milliseconds delay);
    Future& write_async(const char *data, std::size_t length);
    void advance_read();
    void advance_write();
    void set_read_interest(bool enabled);
    void set_write_interest(bool enabled);

    EventLoop& loop_;
    int fd_;
    bool want_read_ = true;
    bool want_write_ = false;
    int request_number_ = 0;
    ClientState state_ = ClientState::Read;

    char read_buffer_[REQUEST_SIZE];
    std::size_t read_used_ = 0;
    Future operation_;

    char write_buffer_[64];
    std::size_t write_used_ = 0;
    std::size_t write_length_ = 0;

    Task task_;
};

void Future::reset()
{
    state_ = FutureState::Pending;
    result_ = 0;
    error_ = 0;
    waiter_ = {};
}

void Future::set_result(ssize_t result)
{
    if (state_ != FutureState::Pending) return;
    state_ = FutureState::Finished;
    result_ = result;
    wake_waiter();
}

void Future::set_error(int error)
{
    if (state_ != FutureState::Pending) return;
    state_ = FutureState::Failed;
    error_ = error;
    wake_waiter();
}

void Future::add_waiter(std::function<void()> waiter)
{
    waiter_ = std::move(waiter);
    if (state_ != FutureState::Pending) wake_waiter();
}

void Future::wake_waiter()
{
    if (!waiter_) return;
    loop_.call_soon(std::exchange(waiter_, {}));
}

EventLoop::EventLoop() : epoll_fd_(epoll_create1(0))
{
    if (epoll_fd_ < 0) die("epoll_create1");
}

EventLoop::~EventLoop()
{
    ::close(epoll_fd_);
}

void EventLoop::call_soon(std::function<void()> callback)
{
    ready_.push_back(std::move(callback));
}

void EventLoop::call_later(std::chrono::milliseconds delay, Future& future,
                           std::shared_ptr<Connection> owner)
{
    timers_.push({ std::chrono::steady_clock::now() + delay,
                   &future, std::move(owner) });
}

void EventLoop::sync_epoll(const Connection& connection)
{
    epoll_event event = {};
    event.events = (connection.wants_read() ? EPOLLIN : 0U)
                 | (connection.wants_write() ? EPOLLOUT : 0U);
    event.data.ptr = const_cast<Connection *>(&connection);
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, connection.fd(), &event) < 0)
        die("epoll_ctl modify");
}

void EventLoop::remove_connection(int fd)
{
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    connections_.erase(fd);
}

int EventLoop::poll_timeout() const
{
    if (!ready_.empty()) return 0;
    if (timers_.empty()) return -1;

    auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(
        timers_.top().when - std::chrono::steady_clock::now()).count();
    if (delay <= 0) return 0;
    return delay > INT32_MAX ? INT32_MAX : static_cast<int>(delay);
}

void EventLoop::expire_timers()
{
    auto now = std::chrono::steady_clock::now();
    while (!timers_.empty() && timers_.top().when <= now) {
        Timer expired = timers_.top();
        timers_.pop();
        expired.future->set_result(0);
    }
}

void EventLoop::run_ready_snapshot()
{
    std::size_t count = ready_.size();
    while (count--) {
        auto callback = std::move(ready_.front());
        ready_.pop_front();
        callback();
    }
}

static int make_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    return flags < 0 ? -1 : fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void EventLoop::accept_ready(int listener)
{
    for (;;) {
        int fd = accept(listener, nullptr, nullptr);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            die("accept");
        }
        if (make_nonblocking(fd) < 0) {
            ::close(fd);
            continue;
        }

        auto connection = std::make_shared<Connection>(*this, fd);
        epoll_event event = {};
        event.events = EPOLLIN;
        event.data.ptr = connection.get();
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0)
            die("epoll_ctl add client");

        connections_[fd] = connection;
        call_soon([connection] { connection->task().step(connection); });
    }
}

void EventLoop::run(int listener)
{
    epoll_event listener_event = {};
    listener_event.events = EPOLLIN;
    listener_event.data.ptr = nullptr;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listener, &listener_event) < 0)
        die("epoll_ctl listener");

    epoll_event events[MAX_EVENTS];
    for (;;) {
        int count = epoll_wait(epoll_fd_, events, MAX_EVENTS, poll_timeout());
        if (count < 0) {
            if (errno == EINTR) count = 0;
            else die("epoll_wait");
        }

        for (int i = 0; i < count; ++i) {
            if (events[i].data.ptr == nullptr) {
                accept_ready(listener);
                continue;
            }

            auto *raw = static_cast<Connection *>(events[i].data.ptr);
            if (raw->fd() == -1) continue;
            auto connection = raw->shared_from_this();

            if (events[i].events & EPOLLIN)
                call_soon([connection] { connection->dispatch_read(); });
            if (events[i].events & EPOLLOUT)
                call_soon([connection] { connection->dispatch_write(); });
            if (events[i].events & (EPOLLERR | EPOLLHUP))
                call_soon([connection] { connection->close(); });
        }

        expire_timers();
        run_ready_snapshot();
    }
}

void Task::step(const std::shared_ptr<Connection>& connection)
{
    Future *completed = std::exchange(waiting_on_, nullptr);
    if (completed && completed->state() == FutureState::Failed) {
        connection->close();
        return;
    }

    CoroutineResult yielded = coroutine_.resume(completed);
    if (yielded.action == CoroutineAction::Done) {
        connection->close();
        return;
    }

    waiting_on_ = yielded.awaitable;
    waiting_on_->add_waiter(
        [connection] { connection->task().step(connection); });
}

void Connection::set_read_interest(bool enabled)
{
    if (want_read_ == enabled) return;
    want_read_ = enabled;
    loop_.sync_epoll(*this);
}

void Connection::set_write_interest(bool enabled)
{
    if (want_write_ == enabled) return;
    want_write_ = enabled;
    loop_.sync_epoll(*this);
}

void Connection::close()
{
    if (fd_ == -1) return;

    int closed_fd = fd_;
    loop_.remove_connection(closed_fd);
    ::close(fd_);
    fd_ = -1;

    if (task_.waiting_on()
        && task_.waiting_on()->state() == FutureState::Pending)
        task_.waiting_on()->set_error(ECANCELED);

}

void Connection::advance_read()
{
    while (read_used_ < REQUEST_SIZE) {
        ssize_t count = ::read(fd_, read_buffer_ + read_used_,
                               REQUEST_SIZE - read_used_);
        if (count > 0) {
            read_used_ += static_cast<std::size_t>(count);
            continue;
        }
        if (count == 0) {
            operation_.set_error(ECONNRESET);
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        operation_.set_error(errno);
        break;
    }

    if (read_used_ == REQUEST_SIZE)
        operation_.set_result(static_cast<ssize_t>(read_used_));
    if (operation_.state() != FutureState::Pending)
        set_read_interest(false);
}

Future& Connection::read_exact()
{
    read_used_ = 0;
    operation_.reset();
    advance_read();
    if (operation_.state() == FutureState::Pending)
        set_read_interest(true);
    return operation_;
}

void Connection::advance_write()
{
    while (write_used_ < write_length_) {
        ssize_t count = ::write(fd_, write_buffer_ + write_used_,
                                write_length_ - write_used_);
        if (count > 0) {
            write_used_ += static_cast<std::size_t>(count);
            continue;
        }
        if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;
        operation_.set_error(count < 0 ? errno : EPIPE);
        break;
    }

    if (write_used_ == write_length_)
        operation_.set_result(static_cast<ssize_t>(write_used_));
    set_write_interest(operation_.state() == FutureState::Pending);
}

Future& Connection::write_async(const char *data, std::size_t length)
{
    operation_.reset();
    if (length > sizeof(write_buffer_)) {
        operation_.set_error(EMSGSIZE);
        return operation_;
    }

    std::memcpy(write_buffer_, data, length);
    write_used_ = 0;
    write_length_ = length;
    advance_write();
    return operation_;
}

Future& Connection::sleep_async(std::chrono::milliseconds delay)
{
    operation_.reset();
    loop_.call_later(delay, operation_, shared_from_this());
    return operation_;
}

CoroutineResult Connection::resume(Future *completed)
{
    (void) completed;

    for (;;) {
        switch (state_) {
        case ClientState::Read:
            state_ = ClientState::AfterRead;
            return CoroutineResult::await(read_exact());

        case ClientState::AfterRead:
            ++request_number_;
            state_ = ClientState::AfterSleep;
            return CoroutineResult::await(sleep_async(std::chrono::milliseconds(10)));

        case ClientState::AfterSleep: {
            char reply[64];
            int length = std::snprintf(reply, sizeof(reply), "hi %d\n", request_number_);
            state_ = ClientState::AfterWrite;
            return CoroutineResult::await(
                write_async(reply, static_cast<std::size_t>(length)));
        }

        case ClientState::AfterWrite:
            state_ = ClientState::Read;
            break;
        }
    }
}

void Connection::dispatch_read()
{
    if (fd_ != -1 && want_read_) advance_read();
}

void Connection::dispatch_write()
{
    if (fd_ != -1 && want_write_) advance_write();
}

static int create_listener()
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) die("socket");

    int yes = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in address = {};
    address.sin_family = AF_INET;
    address.sin_port = htons(PORT);
    address.sin_addr.s_addr = htonl(INADDR_ANY);

    if (make_nonblocking(fd) < 0) die("nonblocking listener");
    if (bind(fd, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0)
        die("bind");
    if (listen(fd, 256) < 0) die("listen");
    return fd;
}

int main()
{
    signal(SIGPIPE, SIG_IGN);
    int listener = create_listener();
    std::printf("C++ async server listening on :%d\n", PORT);

    EventLoop loop;
    loop.run(listener);
}
