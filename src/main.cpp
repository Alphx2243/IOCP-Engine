#include "application/protocol_router.hpp"
#include "core/iocp_server.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>

#ifdef _WIN32
#include <windows.h>
#else
#include <csignal> 
#endif

namespace {
std::atomic<bool> stop_requested = false;
std::mutex stop_mutex;
std::condition_variable stop_cv;

void RequestStop(){
    stop_requested = true, stop_cv.notify_all();
}

#ifdef _WIN32
BOOL WINAPI ConsoleHandler(DWORD signal) {
    if (signal == CTRL_C_EVENT || signal == CTRL_CLOSE_EVENT || signal == CTRL_SHUTDOWN_EVENT) {
        RequestStop();
        return TRUE;
    }
    return FALSE;
}
#else
void SignalHandler(int) {
    RequestStop();
}
#endif

std::optional<std::string> Environment(const char* name) {
#ifdef _WIN32
    char* value = nullptr;
    std::size_t length = 0;
    if (_dupenv_s(&value, &length, name) or value == nullptr) return std::nullopt;
    std::string result(value);
    std::free(value);
    return result;
#else
    if (const char* value = std::getenv(name)) return std::string(value);
    return std::nullopt;
#endif
}

std::size_t ParseSize(const std::string& value, const char* option) { // option is just for better display (error msg)
    std::size_t consumed = 0;
    const auto parsed = std::stoull(value, &consumed);
    if (consumed != value.size()) throw std::invalid_argument(std::string("invalid ") + option);
    return static_cast<std::size_t>(parsed);
}

bool ConstantTimeEqual(std::string_view first, std::string_view second) {
    // Compare two strings in constant time so that an attacker won't be able to determine where they differ based on execution time.
    std::size_t difference = first.size() ^ second.size();
    const auto length = std::max(first.size(), second.size());
    for (std::size_t index = 0; index < length; index++) {
        const char left = index < first.size() ? first[index] : 0;
        const char right = index < second.size() ? second[index] : 0;
        difference |= static_cast<unsigned char>(left ^ right);
    }
    return difference == 0;
}
}

int main(int argc, char** argv) {
    RealtimeEngine::ServerConfig config;
    std::string auth_token = Environment("IOCP_AUTH_TOKEN").value_or("development-only-change-me");
    std::string allowed_origin = Environment("IOCP_ALLOWED_ORIGIN").value_or("");
    std::chrono::milliseconds metrics_interval{5000};
    try {
        for (int index = 1; index < argc; index++) {
            const std::string option = argv[index];
            const auto require_value = [&]() -> std::string {
                if (++index >= argc) throw std::invalid_argument("missing value for " + option);
                return argv[index];
            };
            if (option == "--port") {
                const auto port = ParseSize(require_value(), "--port");
                if (port == 0 || port > 65535) throw std::invalid_argument("port must be between 1 and 65535");
                config.port = static_cast<std::uint16_t>(port);
            } 
            else if (option == "--workers") config.worker_count = ParseSize(require_value(), "--workers");
            else if (option == "--max-connections") config.max_connections = ParseSize(require_value(), "--max-connections");
            else if (option == "--max-message-bytes") config.max_message_bytes = ParseSize(require_value(), "--max-message-bytes");
            else if (option == "--session-queue-bytes") config.max_session_queued_bytes = ParseSize(require_value(), "--session-queue-bytes");
            else if (option == "--global-queue-bytes") {
                config.max_global_queued_bytes = ParseSize(require_value(), "--global-queue-bytes");
            } 
            else if (option == "--auth-token") auth_token = require_value();
            else if (option == "--allowed-origin") allowed_origin = require_value();
            else if (option == "--metrics-interval-ms") {
                metrics_interval = std::chrono::milliseconds(ParseSize(require_value(), "--metrics-interval-ms"));
            } 
            else if (option == "--thread-affinity") config.enable_thread_affinity = true;
            else throw std::invalid_argument("unknown option: " + option);
        }
    } 
    catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
    if (allowed_origin.size() != 0) {
        config.origin_validator =
            [allowed_origin](std::string_view origin) {
                return origin == allowed_origin;
            };
    }
    RealtimeEngine::IocpServer server(config);
    RealtimeEngine::ProtocolRouter router(server,
        [auth_token](std::string_view token) -> std::optional<std::string> {
            if (!ConstantTimeEqual(token, auth_token)) return std::nullopt;  
            return std::string("service-user");
        });
    server.RegisterMessageHandler(
        [&router](RealtimeEngine::SessionHandle session, std::span<const std::uint8_t> payload) {
            return router.Handle(session, payload);
        });
    server.RegisterDisconnectHandler(
        [&router](RealtimeEngine::SessionHandle session) {
            router.OnDisconnect(session);
        });

#ifdef _WIN32
    SetConsoleCtrlHandler(ConsoleHandler, TRUE);
#else
    std::signal(SIGINT, SignalHandler);
    std::signal(SIGTERM, SignalHandler);
#endif

    if (!server.Initialize() || !server.Start()) {
        std::cerr << "{\"event\":\"startup_failed\"}\n";
        return 1;
    }
    std::cout << "{\"event\":\"ready\",\"ready\":true,\"port\":" << config.port << "}\n";

    std::unique_lock lock(stop_mutex);
    while (stop_requested == false) {
        stop_cv.wait_for(lock, metrics_interval);
        if (stop_requested.load()) break;
        const auto metrics = server.Metrics();
        std::cout
            << "{\"event\":\"metrics\",\"active_connections\":" << metrics.active_connections
            << ",\"accepted_connections\":" << metrics.accepted_connections
            << ",\"received_messages\":" << metrics.received_messages << ",\"sent_messages\":" << metrics.sent_messages
            << ",\"malformed_messages\":" << metrics.malformed_messages
            << ",\"send_failures\":" << metrics.send_failures << ",\"queued_bytes\":" << metrics.queued_bytes
            << ",\"inbound_bytes\":" << metrics.inbound_bytes << "}\n";
    }
    lock.unlock();
    server.Stop();
    return 0;
}