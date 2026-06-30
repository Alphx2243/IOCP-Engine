#include "core/iocp_server.hpp"

#include "core/memory_pool.hpp"
#include "core/send_cursor.hpp"
#include "protocol/framing.hpp"
#include "transport/websocket_transport.hpp"

#include <algorithm>
#include <array>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <iostream>
#include <limits>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#include <windows.h>
#endif

namespace RealtimeEngine {
namespace {

std::int64_t MonotonicMilliseconds() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

struct AtomicMetrics {
    std::atomic<std::uint64_t> accepted_connections{0};
    std::atomic<std::uint64_t> active_connections{0};
    std::atomic<std::uint64_t> received_messages{0};
    std::atomic<std::uint64_t> sent_messages{0};
    std::atomic<std::uint64_t> malformed_messages{0};
    std::atomic<std::uint64_t> rejected_connections{0};
    std::atomic<std::uint64_t> send_failures{0};
    std::atomic<std::uint64_t> queued_bytes{0};
    std::atomic<std::uint64_t> inbound_bytes{0};

    ServerMetricsSnapshot Snapshot() const noexcept {
        return {
            accepted_connections.load(std::memory_order_relaxed),
            active_connections.load(std::memory_order_relaxed),
            received_messages.load(std::memory_order_relaxed),
            sent_messages.load(std::memory_order_relaxed),
            malformed_messages.load(std::memory_order_relaxed),
            rejected_connections.load(std::memory_order_relaxed),
            send_failures.load(std::memory_order_relaxed),
            queued_bytes.load(std::memory_order_relaxed),
            inbound_bytes.load(std::memory_order_relaxed)
        };
    }
};

}

#ifdef _WIN32
namespace {

class WinsockRuntime {
public:
    bool Start() {
        if (started_) return true;
        WSADATA data{};
        const int result = WSAStartup(MAKEWORD(2, 2), &data);
        started_ = result == 0;
        return started_;
    }

    ~WinsockRuntime() {
        if (started_) WSACleanup();
    }

private:
    bool started_ = false;
};

class UniqueSocket {
public:
    UniqueSocket() = default;
    explicit UniqueSocket(SOCKET socket) : socket_(socket) {}
    ~UniqueSocket() { Reset(); }
    UniqueSocket(const UniqueSocket&) = delete;
    UniqueSocket& operator=(const UniqueSocket&) = delete;
    UniqueSocket(UniqueSocket&& other) noexcept
        : socket_(std::exchange(other.socket_, INVALID_SOCKET)) {}
    UniqueSocket& operator=(UniqueSocket&& other) noexcept {
        if (this != &other) {
            Reset(std::exchange(other.socket_, INVALID_SOCKET));
        }
        return *this;
    }
    SOCKET Get() const noexcept { return socket_; }
    SOCKET Release() noexcept { return std::exchange(socket_, INVALID_SOCKET); }
    void Reset(SOCKET socket = INVALID_SOCKET) noexcept {
        if (socket_ != INVALID_SOCKET) closesocket(socket_);
        socket_ = socket;
    }
    explicit operator bool() const noexcept { return socket_ != INVALID_SOCKET; }

private:
    SOCKET socket_ = INVALID_SOCKET;
};

class UniqueHandle {
public:
    UniqueHandle() = default;
    explicit UniqueHandle(HANDLE handle) : handle_(handle) {}
    ~UniqueHandle() { Reset(); }
    UniqueHandle(const UniqueHandle&) = delete;
    UniqueHandle& operator=(const UniqueHandle&) = delete;
    HANDLE Get() const noexcept { return handle_; }
    HANDLE Release() noexcept { return std::exchange(handle_, nullptr); }
    void Reset(HANDLE handle = nullptr) noexcept {
        if (handle_ != nullptr) CloseHandle(handle_);
        handle_ = handle;
    }
    explicit operator bool() const noexcept { return handle_ != nullptr; }

private:
    HANDLE handle_ = nullptr;
};

enum class IoOperation {
    Accept,
    Read,
    Write
};

struct OperationContext {
    OVERLAPPED overlapped{};
    IoOperation operation;
};

static_assert(std::is_standard_layout_v<OperationContext>);
static_assert(offsetof(OperationContext, overlapped) == 0);

struct AcceptOperation {
    explicit AcceptOperation(SOCKET socket_value)
        : context{{}, IoOperation::Accept}, socket(socket_value) {}
    OperationContext context;
    SOCKET socket = INVALID_SOCKET;
    std::array<char, 2 * (sizeof(sockaddr_in6) + 16)> addresses{};
};

static_assert(std::is_standard_layout_v<AcceptOperation>);
static_assert(offsetof(AcceptOperation, context) == 0);

struct OutboundItem {
    std::array<std::uint8_t, 16> header{};
    std::size_t header_size = 0;
    SharedBuffer payload;
    bool close_after = false;
};

struct ClientSession {
    ClientSession(SessionHandle session_handle, SOCKET socket_value,
                  const ServerConfig& config)
        : handle(session_handle),
          socket(socket_value),
          raw_decoder(config.max_message_bytes,
                      config.max_session_inbound_bytes),
          websocket_decoder(config.max_message_bytes,
                            config.max_session_inbound_bytes) {
        const auto now = MonotonicMilliseconds();
        connected_at_ms.store(now);
        last_activity_ms.store(now);
        last_pong_ms.store(now);
        rate_window_ms = now;
    }

    SessionHandle handle;
    std::atomic<SOCKET> socket{INVALID_SOCKET};
    std::atomic<SessionState> state{SessionState::Connecting};
    std::atomic<TransportKind> transport{TransportKind::Detecting};
    std::atomic<bool> disconnect_started{false};
    LengthPrefixedAccumulator raw_decoder;
    WebSocketDecoder websocket_decoder;
    std::vector<std::uint8_t> detection_buffer;
    std::size_t inbound_accounted = 0;
    std::uint32_t malformed_messages = 0;
    std::atomic<std::int64_t> connected_at_ms{0};
    std::atomic<std::int64_t> last_activity_ms{0};
    std::atomic<std::int64_t> last_pong_ms{0};
    std::atomic<std::int64_t> close_started_ms{0};
    std::atomic<bool> awaiting_pong{false};
    std::int64_t rate_window_ms = 0;
    std::size_t rate_window_messages = 0;
    std::mutex send_mutex;
    std::deque<OutboundItem> send_queue;
    std::size_t queued_bytes = 0;
    bool write_active = false;
};

struct ReadOperation {
    explicit ReadOperation(std::shared_ptr<ClientSession> owner,
                           std::size_t buffer_size)
        : context{{}, IoOperation::Read},
          session(std::move(owner)),
          buffer(buffer_size) {
        wsabuf.buf = reinterpret_cast<char*>(buffer.data());
        wsabuf.len = static_cast<ULONG>(buffer.size());
    }
    OperationContext context;
    std::shared_ptr<ClientSession> session;
    WSABUF wsabuf{};
    std::vector<std::uint8_t> buffer;
};

static_assert(std::is_standard_layout_v<ReadOperation>);
static_assert(offsetof(ReadOperation, context) == 0);

struct WriteOperation {
    WriteOperation(std::shared_ptr<ClientSession> owner, OutboundItem item)
        : context{{}, IoOperation::Write},
          session(std::move(owner)),
          cursor(std::span(item.header).first(item.header_size),
                 std::move(item.payload)),
          close_after(item.close_after) {}
    OperationContext context;
    std::shared_ptr<ClientSession> session;
    SendCursor cursor;
    std::array<WSABUF, 2> buffers{};
    bool close_after = false;
};

static_assert(std::is_standard_layout_v<WriteOperation>);
static_assert(offsetof(WriteOperation, context) == 0);

template <typename Value>
class ShardedSessionRegistry {
public:
    void Insert(SessionHandle handle, Value value) {
        auto& shard = shards_[handle.id % ShardCount];
        std::unique_lock lock(shard.mutex);
        shard.values[handle.id] = std::move(value);
    }

    Value Find(SessionHandle handle) const {
        auto& shard = shards_[handle.id % ShardCount];
        std::shared_lock lock(shard.mutex);
        const auto it = shard.values.find(handle.id);
        if (it == shard.values.end() ||
            it->second->handle.generation != handle.generation) {
            return {};
        }
        return it->second;
    }

    void Remove(SessionHandle handle) {
        auto& shard = shards_[handle.id % ShardCount];
        std::unique_lock lock(shard.mutex);
        const auto it = shard.values.find(handle.id);
        if (it != shard.values.end() &&
            it->second->handle.generation == handle.generation) {
            shard.values.erase(it);
        }
    }

    std::vector<Value> Snapshot() const {
        std::vector<Value> result;
        for (auto& shard : shards_) {
            std::shared_lock lock(shard.mutex);
            result.reserve(result.size() + shard.values.size());
            for (const auto& [id, value] : shard.values) {
                (void)id;
                result.push_back(value);
            }
        }
        return result;
    }

private:
    static constexpr std::size_t ShardCount = 32;
    struct Shard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::uint64_t, Value> values;
    };
    mutable std::array<Shard, ShardCount> shards_;
};

}

class IocpServer::Impl {
public:
    explicit Impl(ServerConfig config_value)
        : config(std::move(config_value)) {
        if (config.worker_count == 0) {
            config.worker_count =
                std::max<std::size_t>(1, std::thread::hardware_concurrency());
        }
        config.accept_depth = std::max<std::size_t>(1, config.accept_depth);
        config.receive_buffer_bytes =
            std::max<std::size_t>(1024, config.receive_buffer_bytes);
    }

    ~Impl() { Stop(); }

    bool Initialize() {
        std::lock_guard lock(lifecycle_mutex);
        if (initialized) return true;
        if (!winsock.Start()) {
            LogError("WSAStartup", WSAGetLastError());
            return false;
        }

        UniqueHandle new_port(CreateIoCompletionPort(
            INVALID_HANDLE_VALUE, nullptr, 0,
            static_cast<DWORD>(config.worker_count)));
        if (!new_port) {
            LogError("CreateIoCompletionPort", GetLastError());
            return false;
        }

        UniqueSocket new_listen(WSASocketW(
            AF_INET6, SOCK_STREAM, IPPROTO_TCP, nullptr, 0,
            WSA_FLAG_OVERLAPPED));
        if (!new_listen) {
            LogError("WSASocketW", WSAGetLastError());
            return false;
        }
        DWORD ipv6_only = 0;
        if (setsockopt(new_listen.Get(), IPPROTO_IPV6, IPV6_V6ONLY,
                       reinterpret_cast<const char*>(&ipv6_only),
                       sizeof(ipv6_only)) == SOCKET_ERROR) {
            LogError("setsockopt(IPV6_V6ONLY)", WSAGetLastError());
            return false;
        }

        sockaddr_in6 address{};
        address.sin6_family = AF_INET6;
        address.sin6_addr = in6addr_any;
        address.sin6_port = htons(config.port);
        if (bind(new_listen.Get(), reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) == SOCKET_ERROR) {
            LogError("bind", WSAGetLastError());
            return false;
        }
        if (listen(new_listen.Get(), SOMAXCONN) == SOCKET_ERROR) {
            LogError("listen", WSAGetLastError());
            return false;
        }
        if (CreateIoCompletionPort(
                reinterpret_cast<HANDLE>(new_listen.Get()), new_port.Get(), 0,
                0) == nullptr) {
            LogError("associate listen socket", GetLastError());
            return false;
        }

        GUID accept_ex_guid = WSAID_ACCEPTEX;
        GUID addresses_guid = WSAID_GETACCEPTEXSOCKADDRS;
        DWORD bytes = 0;
        if (WSAIoctl(new_listen.Get(), SIO_GET_EXTENSION_FUNCTION_POINTER,
                     &accept_ex_guid, sizeof(accept_ex_guid), &accept_ex,
                     sizeof(accept_ex), &bytes, nullptr, nullptr) ==
                SOCKET_ERROR ||
            WSAIoctl(new_listen.Get(), SIO_GET_EXTENSION_FUNCTION_POINTER,
                     &addresses_guid, sizeof(addresses_guid),
                     &get_accept_ex_sockaddrs,
                     sizeof(get_accept_ex_sockaddrs), &bytes, nullptr,
                     nullptr) == SOCKET_ERROR) {
            LogError("WSAIoctl(AcceptEx)", WSAGetLastError());
            return false;
        }

        accept_pool =
            std::make_unique<TypedObjectPool<AcceptOperation>>(
                config.accept_depth);
        completion_port.Reset(new_port.Release());
        listen_socket.Reset(new_listen.Release());
        initialized = true;
        return true;
    }

    bool Start() {
        {
            std::lock_guard lock(lifecycle_mutex);
            if (!initialized || running.load()) return running.load();
            running.store(true);
            stopping.store(false);
        }

        try {
            workers.reserve(config.worker_count);
            for (std::size_t index = 0; index < config.worker_count; ++index) {
                workers.emplace_back([this, index] { WorkerLoop(index); });
            }
            timer_thread = std::thread([this] { TimerLoop(); });
        } catch (...) {
            Stop();
            return false;
        }

        std::size_t posted = 0;
        for (std::size_t index = 0; index < config.accept_depth; ++index) {
            if (PostNewAccept()) ++posted;
        }
        if (posted == 0) {
            Stop();
            return false;
        }
        std::cout << "{\"event\":\"server_started\",\"port\":" << config.port
                  << ",\"workers\":" << config.worker_count
                  << ",\"accept_depth\":" << posted << "}\n";
        return true;
    }

    void Stop() {
        bool expected = true;
        if (!running.compare_exchange_strong(expected, false)) {
            return;
        }
        stopping.store(true);
        timer_cv.notify_all();
        if (timer_thread.joinable()) timer_thread.join();

        if (listen_socket) {
            CancelIoEx(reinterpret_cast<HANDLE>(listen_socket.Get()), nullptr);
            listen_socket.Reset();
        }
        for (const auto& session : sessions.Snapshot()) {
            Disconnect(session);
        }

        {
            std::unique_lock lock(outstanding_mutex);
            outstanding_cv.wait(lock, [this] {
                return outstanding_operations.load() == 0;
            });
        }

        for (std::size_t index = 0; index < workers.size(); ++index) {
            PostQueuedCompletionStatus(completion_port.Get(), 0, 0, nullptr);
        }
        for (auto& worker : workers) {
            if (worker.joinable()) worker.join();
        }
        workers.clear();
        completion_port.Reset();
        accept_pool.reset();
        initialized = false;
        std::cout << "{\"event\":\"server_stopped\"}\n";
    }

    void RegisterMessageHandler(MessageHandler value) {
        std::lock_guard lock(callback_mutex);
        message_handler = std::move(value);
    }

    void RegisterConnectHandler(SessionHandler value) {
        std::lock_guard lock(callback_mutex);
        connect_handler = std::move(value);
    }

    void RegisterDisconnectHandler(SessionHandler value) {
        std::lock_guard lock(callback_mutex);
        disconnect_handler = std::move(value);
    }

    bool Send(SessionHandle handle, SharedBuffer payload) {
        const auto session = sessions.Find(handle);
        if (!session || !payload || payload->empty() ||
            payload->size() > config.max_message_bytes ||
            session->state.load() != SessionState::Open) {
            return false;
        }
        OutboundItem item;
        item.payload = std::move(payload);
        const auto transport = session->transport.load();
        if (transport == TransportKind::RawTcp) {
            const auto header =
                LengthPrefixedAccumulator::EncodeLength(item.payload->size());
            std::copy(header.begin(), header.end(), item.header.begin());
            item.header_size = header.size();
        } else if (transport == TransportKind::WebSocket) {
            item.header = WebSocketHeader(
                WsOpcode::Binary, item.payload->size(), item.header_size);
        } else {
            return false;
        }
        return Enqueue(session, std::move(item));
    }

    void Close(SessionHandle handle, std::uint16_t websocket_code,
               std::string reason) {
        const auto session = sessions.Find(handle);
        if (!session) return;
        if (session->transport.load() != TransportKind::WebSocket) {
            Disconnect(session);
            return;
        }
        if (reason.size() > 123) reason.resize(123);
        std::vector<std::uint8_t> payload;
        payload.reserve(2 + reason.size());
        payload.push_back(
            static_cast<std::uint8_t>((websocket_code >> 8U) & 0xffU));
        payload.push_back(static_cast<std::uint8_t>(websocket_code & 0xffU));
        payload.insert(payload.end(), reason.begin(), reason.end());
        SendControl(session, WsOpcode::Close, std::move(payload), true);
    }

    bool IsRunning() const noexcept { return running.load(); }
    ServerMetricsSnapshot Metrics() const noexcept {
        return metrics.Snapshot();
    }

private:
    static std::array<std::uint8_t, 16> WebSocketHeader(
        WsOpcode opcode, std::size_t payload_size, std::size_t& header_size) {
        std::size_t temporary_size = 0;
        const auto compact = WebSocketTransport::BuildFrameHeader(
            opcode, payload_size, temporary_size);
        std::array<std::uint8_t, 16> result{};
        std::copy_n(compact.begin(), temporary_size, result.begin());
        header_size = temporary_size;
        return result;
    }

    void TrackOperation() {
        outstanding_operations.fetch_add(1, std::memory_order_relaxed);
    }

    void CompleteOperation() {
        if (outstanding_operations.fetch_sub(
                1, std::memory_order_acq_rel) == 1) {
            std::lock_guard lock(outstanding_mutex);
            outstanding_cv.notify_all();
        }
    }

    bool PostNewAccept() {
        if (!running.load() || !listen_socket) return false;
        UniqueSocket socket(WSASocketW(
            AF_INET6, SOCK_STREAM, IPPROTO_TCP, nullptr, 0,
            WSA_FLAG_OVERLAPPED));
        if (!socket) {
            LogError("WSASocketW(accept)", WSAGetLastError());
            return false;
        }
        auto* operation = accept_pool->Create(socket.Release());
        if (!operation) {
            metrics.rejected_connections.fetch_add(1);
            return false;
        }
        TrackOperation();
        if (!PostAccept(operation)) {
            accept_pool->Destroy(operation);
            CompleteOperation();
            return false;
        }
        return true;
    }

    bool PostAccept(AcceptOperation* operation) {
        std::memset(&operation->context.overlapped, 0, sizeof(OVERLAPPED));
        DWORD bytes = 0;
        const DWORD address_size =
            static_cast<DWORD>(sizeof(sockaddr_in6) + 16);
        const BOOL result = accept_ex(
            listen_socket.Get(), operation->socket,
            operation->addresses.data(), 0, address_size, address_size,
            &bytes, &operation->context.overlapped);
        if (result) return true;
        const int error = WSAGetLastError();
        if (error == WSA_IO_PENDING) return true;
        LogError("AcceptEx", error);
        return false;
    }

    bool Associate(SOCKET socket) {
        return CreateIoCompletionPort(reinterpret_cast<HANDLE>(socket),
                                      completion_port.Get(), 0, 0) != nullptr;
    }

    void HandleAccept(AcceptOperation* operation, bool succeeded) {
        UniqueSocket accepted(operation->socket);
        operation->socket = INVALID_SOCKET;
        if (succeeded && running.load()) {
            if (metrics.active_connections.load() >= config.max_connections) {
                metrics.rejected_connections.fetch_add(1);
            } else {
            const SOCKET listen = listen_socket.Get();
            if (setsockopt(accepted.Get(), SOL_SOCKET, SO_UPDATE_ACCEPT_CONTEXT,
                           reinterpret_cast<const char*>(&listen),
                           sizeof(listen)) == 0 &&
                Associate(accepted.Get())) {
                const auto id = next_session_id.fetch_add(1);
                const auto generation = next_generation.fetch_add(1);
                const SessionHandle handle{id, generation};
                auto session = std::make_shared<ClientSession>(
                    handle, accepted.Release(), config);
                session->state.store(SessionState::Handshaking);
                sessions.Insert(handle, session);
                metrics.accepted_connections.fetch_add(1);
                metrics.active_connections.fetch_add(1);
                InvokeConnect(handle);
                if (!PostRead(session)) {
                    Disconnect(session);
                }
            } else {
                metrics.rejected_connections.fetch_add(1);
            }
            }
        }

        if (running.load()) {
            UniqueSocket next(WSASocketW(
                AF_INET6, SOCK_STREAM, IPPROTO_TCP, nullptr, 0,
                WSA_FLAG_OVERLAPPED));
            if (next) {
                operation->socket = next.Release();
                if (PostAccept(operation)) return;
                closesocket(operation->socket);
                operation->socket = INVALID_SOCKET;
            }
        }
        accept_pool->Destroy(operation);
        CompleteOperation();
    }

    bool PostRead(const std::shared_ptr<ClientSession>& session) {
        auto* operation =
            new (std::nothrow) ReadOperation(session, config.receive_buffer_bytes);
        if (!operation) return false;
        TrackOperation();
        if (!PostRead(operation)) {
            delete operation;
            CompleteOperation();
            return false;
        }
        return true;
    }

    bool PostRead(ReadOperation* operation) {
        const SOCKET socket = operation->session->socket.load();
        if (socket == INVALID_SOCKET) return false;
        std::memset(&operation->context.overlapped, 0, sizeof(OVERLAPPED));
        operation->wsabuf.buf =
            reinterpret_cast<char*>(operation->buffer.data());
        operation->wsabuf.len =
            static_cast<ULONG>(operation->buffer.size());
        DWORD flags = 0;
        DWORD bytes = 0;
        const int result = WSARecv(
            socket, &operation->wsabuf, 1, &bytes, &flags,
            &operation->context.overlapped, nullptr);
        if (result == 0) return true;
        return WSAGetLastError() == WSA_IO_PENDING;
    }

    void HandleRead(ReadOperation* operation, bool succeeded,
                    DWORD bytes_transferred) {
        const auto session = operation->session;
        if (!succeeded || bytes_transferred == 0 ||
            session->disconnect_started.load()) {
            Disconnect(session);
            delete operation;
            CompleteOperation();
            return;
        }
        session->last_activity_ms.store(MonotonicMilliseconds());
        ProcessIncoming(session, std::span(
            operation->buffer.data(), bytes_transferred));
        if (!session->disconnect_started.load() && running.load() &&
            PostRead(operation)) {
            return;
        }
        Disconnect(session);
        delete operation;
        CompleteOperation();
    }

    bool ReserveGlobalQueued(std::size_t bytes) {
        auto current = metrics.queued_bytes.load();
        while (true) {
            if (bytes > config.max_global_queued_bytes ||
                current > config.max_global_queued_bytes - bytes) {
                return false;
            }
            if (metrics.queued_bytes.compare_exchange_weak(
                    current, current + bytes)) {
                return true;
            }
        }
    }

    bool Enqueue(const std::shared_ptr<ClientSession>& session,
                 OutboundItem item) {
        const auto bytes = item.header_size + item.payload->size();
        bool start_write = false;
        {
            std::lock_guard lock(session->send_mutex);
            if (session->disconnect_started.load()) return false;
            if (bytes > config.max_session_queued_bytes ||
                session->queued_bytes >
                    config.max_session_queued_bytes - bytes ||
                !ReserveGlobalQueued(bytes)) {
                metrics.send_failures.fetch_add(1);
                return false;
            }
            session->queued_bytes += bytes;
            session->send_queue.push_back(std::move(item));
            if (!session->write_active) {
                session->write_active = true;
                start_write = true;
            }
        }
        if (start_write) StartNextWrite(session);
        return true;
    }

    void StartNextWrite(const std::shared_ptr<ClientSession>& session) {
        OutboundItem item;
        {
            std::lock_guard lock(session->send_mutex);
            if (session->send_queue.empty()) {
                session->write_active = false;
                return;
            }
            item = std::move(session->send_queue.front());
            session->send_queue.pop_front();
        }
        auto* operation =
            new (std::nothrow) WriteOperation(session, std::move(item));
        if (!operation) {
            const auto bytes = item.header_size + item.payload->size();
            {
                std::lock_guard lock(session->send_mutex);
                session->queued_bytes -= bytes;
            }
            metrics.queued_bytes.fetch_sub(bytes);
            metrics.send_failures.fetch_add(1);
            Disconnect(session);
            return;
        }
        TrackOperation();
        if (!PostWrite(operation)) {
            FinishWrite(operation, false);
        }
    }

    bool PostWrite(WriteOperation* operation) {
        const SOCKET socket = operation->session->socket.load();
        if (socket == INVALID_SOCKET) return false;
        std::memset(&operation->context.overlapped, 0, sizeof(OVERLAPPED));
        DWORD count = 0;
        const auto header = operation->cursor.HeaderRemainder();
        const auto payload = operation->cursor.PayloadRemainder();
        if (!header.empty()) {
            operation->buffers[count].buf =
                reinterpret_cast<char*>(
                    const_cast<std::uint8_t*>(header.data()));
            operation->buffers[count].len =
                static_cast<ULONG>(header.size());
            ++count;
        }
        if (!payload.empty()) {
            operation->buffers[count].buf =
                reinterpret_cast<char*>(
                    const_cast<std::uint8_t*>(payload.data()));
            operation->buffers[count].len =
                static_cast<ULONG>(payload.size());
            ++count;
        }
        DWORD bytes = 0;
        const int result = WSASend(
            socket, operation->buffers.data(), count, &bytes, 0,
            &operation->context.overlapped, nullptr);
        if (result == 0) return true;
        return WSAGetLastError() == WSA_IO_PENDING;
    }

    void HandleWrite(WriteOperation* operation, bool succeeded,
                     DWORD bytes_transferred) {
        if (!succeeded || bytes_transferred == 0) {
            FinishWrite(operation, false);
            return;
        }
        operation->cursor.Consume(bytes_transferred);
        if (!operation->cursor.Complete()) {
            if (PostWrite(operation)) return;
            FinishWrite(operation, false);
            return;
        }
        FinishWrite(operation, true);
    }

    void FinishWrite(WriteOperation* operation, bool succeeded) {
        const auto session = operation->session;
        const auto bytes = operation->cursor.TotalSize();
        const bool close_after = operation->close_after;
        {
            std::lock_guard lock(session->send_mutex);
            session->queued_bytes =
                bytes > session->queued_bytes ? 0
                                              : session->queued_bytes - bytes;
        }
        metrics.queued_bytes.fetch_sub(bytes);
        if (succeeded) {
            metrics.sent_messages.fetch_add(1);
        } else {
            metrics.send_failures.fetch_add(1);
        }
        delete operation;
        CompleteOperation();

        if (!succeeded || close_after) {
            Disconnect(session);
            return;
        }
        StartNextWrite(session);
    }

    void SendControl(const std::shared_ptr<ClientSession>& session,
                     WsOpcode opcode, std::vector<std::uint8_t> payload,
                     bool close_after) {
        if (close_after) {
            auto current = session->state.load();
            while (current != SessionState::Closed &&
                   current != SessionState::Closing) {
                if (session->state.compare_exchange_weak(
                        current, SessionState::Closing)) {
                    session->close_started_ms.store(MonotonicMilliseconds());
                    break;
                }
            }
            if (current == SessionState::Closed ||
                current == SessionState::Closing) return;
        }
        OutboundItem item;
        item.payload =
            std::make_shared<const std::vector<std::uint8_t>>(
                std::move(payload));
        item.header = WebSocketHeader(
            opcode, item.payload->size(), item.header_size);
        item.close_after = close_after;
        if (!Enqueue(session, std::move(item)) && close_after) {
            Disconnect(session);
        }
    }

    void SendRaw(const std::shared_ptr<ClientSession>& session,
                 std::string response, bool close_after) {
        OutboundItem item;
        item.payload = std::make_shared<const std::vector<std::uint8_t>>(
            response.begin(), response.end());
        item.close_after = close_after;
        if (!Enqueue(session, std::move(item)) && close_after) {
            Disconnect(session);
        }
    }

    void ProcessIncoming(const std::shared_ptr<ClientSession>& session,
                         std::span<const std::uint8_t> bytes) {
        const auto transport = session->transport.load();
        if (transport == TransportKind::Detecting) {
            ProcessDetection(session, bytes);
        } else if (transport == TransportKind::RawTcp) {
            ProcessRaw(session, bytes);
        } else {
            ProcessWebSocket(session, bytes);
        }
    }

    void ProcessDetection(const std::shared_ptr<ClientSession>& session,
                          std::span<const std::uint8_t> bytes) {
        if (session->detection_buffer.size() >
                config.max_session_inbound_bytes ||
            bytes.size() > config.max_session_inbound_bytes -
                               session->detection_buffer.size()) {
            Disconnect(session);
            return;
        }
        session->detection_buffer.insert(session->detection_buffer.end(),
                                         bytes.begin(), bytes.end());
        UpdateInboundAccounting(session, session->detection_buffer.size());

        constexpr std::array<std::uint8_t, 4> GetPrefix{'G', 'E', 'T', ' '};
        const auto compare_length =
            std::min(session->detection_buffer.size(), GetPrefix.size());
        const bool possible_http = std::equal(
            session->detection_buffer.begin(),
            session->detection_buffer.begin() + compare_length,
            GetPrefix.begin());
        if (!possible_http) {
            session->transport.store(TransportKind::RawTcp);
            session->state.store(SessionState::Open);
            auto pending = std::move(session->detection_buffer);
            session->detection_buffer.clear();
            UpdateInboundAccounting(session, 0);
            ProcessRaw(session, pending);
            return;
        }
        if (session->detection_buffer.size() < GetPrefix.size()) return;

        const auto result = WebSocketTransport::ParseHandshake(
            session->detection_buffer, config.max_handshake_bytes,
            config.origin_validator);
        if (result.status == HandshakeStatus::NeedMoreData) return;
        if (result.status == HandshakeStatus::Rejected) {
            SendRaw(session, result.response, true);
            return;
        }

        SendRaw(session, result.response, false);
        session->transport.store(TransportKind::WebSocket);
        session->state.store(SessionState::Open);
        std::vector<std::uint8_t> remaining;
        if (result.consumed < session->detection_buffer.size()) {
            remaining.assign(
                session->detection_buffer.begin() + result.consumed,
                session->detection_buffer.end());
        }
        session->detection_buffer.clear();
        UpdateInboundAccounting(session, 0);
        if (!remaining.empty()) ProcessWebSocket(session, remaining);
    }

    void ProcessRaw(const std::shared_ptr<ClientSession>& session,
                    std::span<const std::uint8_t> bytes) {
        if (!session->raw_decoder.Append(bytes)) {
            Disconnect(session);
            return;
        }
        UpdateInboundAccounting(session, session->raw_decoder.BufferedBytes());
        while (!session->disconnect_started.load()) {
            std::span<const std::uint8_t> message;
            const auto status = session->raw_decoder.Next(message);
            if (status == FrameDecodeStatus::NeedMoreData) break;
            if (status != FrameDecodeStatus::MessageReady) {
                RecordMalformed(session);
                Disconnect(session);
                return;
            }
            if (!DeliverMessage(session, message)) return;
        }
        UpdateInboundAccounting(session, session->raw_decoder.BufferedBytes());
    }

    void ProcessWebSocket(const std::shared_ptr<ClientSession>& session,
                          std::span<const std::uint8_t> bytes) {
        if (session->state.load() == SessionState::Closing) return;
        if (!session->websocket_decoder.Append(bytes)) {
            Close(session->handle, 1009, "receive buffer limit exceeded");
            return;
        }
        UpdateInboundAccounting(
            session, session->websocket_decoder.BufferedBytes());
        while (!session->disconnect_started.load() &&
               session->state.load() == SessionState::Open) {
            WsEvent event;
            std::string error;
            const auto status = session->websocket_decoder.Next(event, error);
            if (status == WsParseStatus::NeedMoreData) break;
            if (status == WsParseStatus::MessageTooLarge ||
                status == WsParseStatus::BufferLimitExceeded) {
                Close(session->handle, 1009, error);
                return;
            }
            if (status == WsParseStatus::ProtocolError) {
                RecordMalformed(session);
                Close(session->handle, 1002, error);
                return;
            }
            switch (event.type) {
                case WsEventType::BinaryMessage:
                    if (!DeliverMessage(session, event.payload)) return;
                    break;
                case WsEventType::Ping:
                    SendControl(session, WsOpcode::Pong,
                                std::move(event.payload), false);
                    break;
                case WsEventType::Pong:
                    session->awaiting_pong.store(false);
                    session->last_pong_ms.store(MonotonicMilliseconds());
                    break;
                case WsEventType::Close:
                    SendControl(session, WsOpcode::Close,
                                std::move(event.payload), true);
                    return;
            }
        }
        UpdateInboundAccounting(
            session, session->websocket_decoder.BufferedBytes());
    }

    bool DeliverMessage(const std::shared_ptr<ClientSession>& session,
                        std::span<const std::uint8_t> message) {
        const auto now = MonotonicMilliseconds();
        if (now - session->rate_window_ms >= 1000) {
            session->rate_window_ms = now;
            session->rate_window_messages = 0;
        }
        if (++session->rate_window_messages >
            config.max_messages_per_second) {
            Close(session->handle, 1008, "message rate limit exceeded");
            return false;
        }

        MessageHandler callback;
        {
            std::lock_guard lock(callback_mutex);
            callback = message_handler;
        }
        if (!callback) return true;
        metrics.received_messages.fetch_add(1);
        const auto disposition = callback(session->handle, message);
        if (disposition == MessageDisposition::Accepted) return true;
        RecordMalformed(session);
        if (disposition == MessageDisposition::Close ||
            session->malformed_messages >= config.malformed_message_limit) {
            Close(session->handle, 1007, "invalid application message");
            return false;
        }
        return true;
    }

    void RecordMalformed(const std::shared_ptr<ClientSession>& session) {
        ++session->malformed_messages;
        metrics.malformed_messages.fetch_add(1);
    }

    void UpdateInboundAccounting(
        const std::shared_ptr<ClientSession>& session, std::size_t bytes) {
        if (bytes > session->inbound_accounted) {
            const auto delta = bytes - session->inbound_accounted;
            const auto total = metrics.inbound_bytes.fetch_add(delta) + delta;
            session->inbound_accounted = bytes;
            if (total > config.max_global_inbound_bytes) {
                Disconnect(session);
            }
        } else if (bytes < session->inbound_accounted) {
            metrics.inbound_bytes.fetch_sub(
                session->inbound_accounted - bytes);
            session->inbound_accounted = bytes;
        }
    }

    void Disconnect(const std::shared_ptr<ClientSession>& session) {
        if (!session ||
            session->disconnect_started.exchange(true)) {
            return;
        }
        session->state.store(SessionState::Closed);
        sessions.Remove(session->handle);
        const SOCKET socket = session->socket.exchange(INVALID_SOCKET);
        if (socket != INVALID_SOCKET) {
            shutdown(socket, SD_BOTH);
            CancelIoEx(reinterpret_cast<HANDLE>(socket), nullptr);
            closesocket(socket);
        }

        {
            std::lock_guard lock(session->send_mutex);
            for (const auto& item : session->send_queue) {
                const auto bytes = item.header_size + item.payload->size();
                metrics.queued_bytes.fetch_sub(bytes);
                session->queued_bytes -= bytes;
            }
            session->send_queue.clear();
        }
        UpdateInboundAccounting(session, 0);
        metrics.active_connections.fetch_sub(1);
        InvokeDisconnect(session->handle);
    }

    void WorkerLoop(std::size_t worker_index) {
        if (config.enable_thread_affinity) {
            GROUP_AFFINITY affinity{};
            const WORD groups = GetActiveProcessorGroupCount();
            if (groups != 0) {
                WORD group = 0;
                std::size_t local_index = worker_index;
                while (group < groups) {
                    const DWORD count = GetActiveProcessorCount(group);
                    if (local_index < count) break;
                    local_index -= count;
                    ++group;
                }
                if (group < groups &&
                    local_index < sizeof(KAFFINITY) * 8) {
                    affinity.Group = group;
                    affinity.Mask =
                        static_cast<KAFFINITY>(1) << local_index;
                    SetThreadGroupAffinity(GetCurrentThread(), &affinity,
                                           nullptr);
                }
            }
        }

        while (true) {
            DWORD bytes = 0;
            ULONG_PTR key = 0;
            OVERLAPPED* overlapped = nullptr;
            const BOOL succeeded = GetQueuedCompletionStatus(
                completion_port.Get(), &bytes, &key, &overlapped, INFINITE);
            (void)key;
            if (overlapped == nullptr) return;
            auto* context =
                reinterpret_cast<OperationContext*>(overlapped);
            switch (context->operation) {
                case IoOperation::Accept:
                    HandleAccept(
                        reinterpret_cast<AcceptOperation*>(context),
                        succeeded != FALSE);
                    break;
                case IoOperation::Read:
                    HandleRead(
                        reinterpret_cast<ReadOperation*>(context),
                        succeeded != FALSE, bytes);
                    break;
                case IoOperation::Write:
                    HandleWrite(
                        reinterpret_cast<WriteOperation*>(context),
                        succeeded != FALSE, bytes);
                    break;
            }
        }
    }

    void TimerLoop() {
        std::unique_lock lock(timer_mutex);
        while (running.load()) {
            timer_cv.wait_for(lock, std::chrono::milliseconds(500));
            if (!running.load()) break;
            lock.unlock();
            const auto now = MonotonicMilliseconds();
            for (const auto& session : sessions.Snapshot()) {
                if (session->disconnect_started.load()) continue;
                const auto state = session->state.load();
                if (state == SessionState::Handshaking &&
                    now - session->connected_at_ms.load() >
                        config.handshake_timeout.count()) {
                    Disconnect(session);
                    continue;
                }
                if (state == SessionState::Closing &&
                    now - session->close_started_ms.load() >
                        config.close_timeout.count()) {
                    Disconnect(session);
                    continue;
                }
                if (state != SessionState::Open) continue;
                if (now - session->last_activity_ms.load() >
                    config.idle_timeout.count()) {
                    Disconnect(session);
                    continue;
                }
                if (session->transport.load() == TransportKind::WebSocket) {
                    if (session->awaiting_pong.load()) {
                        if (now - session->last_pong_ms.load() >
                            config.websocket_pong_timeout.count()) {
                            Disconnect(session);
                        }
                    } else if (now - session->last_pong_ms.load() >
                               config.websocket_ping_interval.count()) {
                        session->awaiting_pong.store(true);
                        session->last_pong_ms.store(now);
                        SendControl(session, WsOpcode::Ping, {}, false);
                    }
                }
            }
            lock.lock();
        }
    }

    void InvokeConnect(SessionHandle handle) {
        SessionHandler callback;
        {
            std::lock_guard lock(callback_mutex);
            callback = connect_handler;
        }
        if (callback) callback(handle);
    }

    void InvokeDisconnect(SessionHandle handle) {
        SessionHandler callback;
        {
            std::lock_guard lock(callback_mutex);
            callback = disconnect_handler;
        }
        if (callback) callback(handle);
    }

    static void LogError(const char* operation, unsigned long error) {
        std::cerr << "{\"event\":\"native_error\",\"operation\":\""
                  << operation << "\",\"error\":" << error << "}\n";
    }

    ServerConfig config;
    WinsockRuntime winsock;
    UniqueHandle completion_port;
    UniqueSocket listen_socket;
    LPFN_ACCEPTEX accept_ex = nullptr;
    LPFN_GETACCEPTEXSOCKADDRS get_accept_ex_sockaddrs = nullptr;
    std::unique_ptr<TypedObjectPool<AcceptOperation>> accept_pool;
    ShardedSessionRegistry<std::shared_ptr<ClientSession>> sessions;
    std::vector<std::thread> workers;
    std::thread timer_thread;
    std::mutex timer_mutex;
    std::condition_variable timer_cv;
    std::mutex lifecycle_mutex;
    std::mutex callback_mutex;
    MessageHandler message_handler;
    SessionHandler connect_handler;
    SessionHandler disconnect_handler;
    std::atomic<bool> running{false};
    std::atomic<bool> stopping{false};
    bool initialized = false;
    std::atomic<std::uint64_t> next_session_id{1};
    std::atomic<std::uint64_t> next_generation{1};
    std::atomic<std::size_t> outstanding_operations{0};
    std::mutex outstanding_mutex;
    std::condition_variable outstanding_cv;
    AtomicMetrics metrics;
};

#else

class IocpServer::Impl {
public:
    explicit Impl(ServerConfig value) : config(std::move(value)) {}
    bool Initialize() {
        std::cerr << "IOCP is unavailable: this server requires Windows.\n";
        return false;
    }
    bool Start() { return false; }
    void Stop() {}
    void RegisterMessageHandler(MessageHandler value) {
        message_handler = std::move(value);
    }
    void RegisterConnectHandler(SessionHandler value) {
        connect_handler = std::move(value);
    }
    void RegisterDisconnectHandler(SessionHandler value) {
        disconnect_handler = std::move(value);
    }
    bool Send(SessionHandle, SharedBuffer) { return false; }
    void Close(SessionHandle, std::uint16_t, std::string) {}
    bool IsRunning() const noexcept { return false; }
    ServerMetricsSnapshot Metrics() const noexcept { return {}; }

private:
    ServerConfig config;
    MessageHandler message_handler;
    SessionHandler connect_handler;
    SessionHandler disconnect_handler;
};

#endif

IocpServer::IocpServer(ServerConfig config)
    : impl_(std::make_unique<Impl>(std::move(config))) {}

IocpServer::~IocpServer() = default;

bool IocpServer::Initialize() { return impl_->Initialize(); }
bool IocpServer::Start() { return impl_->Start(); }
void IocpServer::Stop() { impl_->Stop(); }

void IocpServer::RegisterMessageHandler(MessageHandler handler) {
    impl_->RegisterMessageHandler(std::move(handler));
}
void IocpServer::RegisterConnectHandler(SessionHandler handler) {
    impl_->RegisterConnectHandler(std::move(handler));
}
void IocpServer::RegisterDisconnectHandler(SessionHandler handler) {
    impl_->RegisterDisconnectHandler(std::move(handler));
}

bool IocpServer::Send(SessionHandle session, SharedBuffer payload) {
    return impl_->Send(session, std::move(payload));
}

bool IocpServer::Send(SessionHandle session,
                      std::span<const std::uint8_t> payload) {
    return Send(session,
                std::make_shared<const std::vector<std::uint8_t>>(
                    payload.begin(), payload.end()));
}

void IocpServer::Close(SessionHandle session, std::uint16_t websocket_code,
                       std::string reason) {
    impl_->Close(session, websocket_code, std::move(reason));
}

bool IocpServer::IsRunning() const noexcept { return impl_->IsRunning(); }

ServerMetricsSnapshot IocpServer::Metrics() const noexcept {
    return impl_->Metrics();
}

}
