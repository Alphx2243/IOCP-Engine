#pragma once

#include "core/outbound_queue.hpp"
#include "core/session_handle.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace RealtimeEngine {

enum class SessionState {
    Connecting,
    Handshaking,
    Open,
    Closing,
    Closed
};

enum class TransportKind {
    Detecting,
    RawTcp,
    WebSocket
};

enum class MessageDisposition {
    Accepted,
    Malformed,
    Close
};

struct ServerConfig {
    std::uint16_t port = 8080;
    std::size_t worker_count = 0;
    std::size_t max_connections = 10'000;
    std::size_t accept_depth = 64;
    std::size_t receive_buffer_bytes = 8192;
    std::size_t max_message_bytes = 1024 * 1024;
    std::size_t max_session_inbound_bytes = 2 * 1024 * 1024;
    std::size_t max_global_inbound_bytes = 256 * 1024 * 1024;
    std::size_t max_session_queued_bytes = 4 * 1024 * 1024;
    std::size_t max_global_queued_bytes = 512 * 1024 * 1024;
    std::size_t max_handshake_bytes = 16 * 1024;
    std::size_t max_messages_per_second = 10'000;
    std::uint32_t malformed_message_limit = 3;
    std::chrono::milliseconds handshake_timeout{5000};
    std::chrono::milliseconds idle_timeout{60000};
    std::chrono::milliseconds websocket_ping_interval{20000};
    std::chrono::milliseconds websocket_pong_timeout{10000};
    std::chrono::milliseconds close_timeout{3000};
    bool enable_thread_affinity = false;
    std::function<bool(std::string_view)> origin_validator;
};

struct ServerMetricsSnapshot {
    std::uint64_t accepted_connections = 0;
    std::uint64_t active_connections = 0;
    std::uint64_t received_messages = 0;
    std::uint64_t sent_messages = 0;
    std::uint64_t malformed_messages = 0;
    std::uint64_t rejected_connections = 0;
    std::uint64_t send_failures = 0;
    std::uint64_t queued_bytes = 0;
    std::uint64_t inbound_bytes = 0;
};

class IocpServer {
public:
    using MessageHandler = std::function<MessageDisposition(
        SessionHandle, std::span<const std::uint8_t>)>;
    using SessionHandler = std::function<void(SessionHandle)>;

    explicit IocpServer(ServerConfig config = {});
    ~IocpServer();

    IocpServer(const IocpServer&) = delete;
    IocpServer& operator=(const IocpServer&) = delete;

    bool Initialize();
    bool Start();
    void Stop();

    void RegisterMessageHandler(MessageHandler handler);
    void RegisterConnectHandler(SessionHandler handler);
    void RegisterDisconnectHandler(SessionHandler handler);

    bool Send(SessionHandle session, SharedBuffer payload);
    bool Send(SessionHandle session, std::span<const std::uint8_t> payload);
    void Close(SessionHandle session, std::uint16_t websocket_code = 1000,
               std::string reason = {});

    [[nodiscard]] bool IsRunning() const noexcept;
    [[nodiscard]] ServerMetricsSnapshot Metrics() const noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}
