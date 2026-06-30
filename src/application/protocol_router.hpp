#pragma once

#include "core/iocp_server.hpp"
#include "pubsub/room_registry.hpp"

#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace RealtimeEngine {

class ProtocolRouter {
public:
    using Authenticator =
        std::function<std::optional<std::string>(std::string_view token)>;
    using RoomAuthorizer = std::function<bool(
        std::string_view principal, std::string_view room, bool publishing)>;

    ProtocolRouter(IocpServer& server, Authenticator authenticator,
                   RoomAuthorizer room_authorizer = {});

    MessageDisposition Handle(
        SessionHandle session, std::span<const std::uint8_t> payload);
    void OnDisconnect(SessionHandle session);

private:
    bool IsAuthenticated(SessionHandle session) const;
    std::optional<std::string> Principal(SessionHandle session) const;
    bool AuthorizeRoom(SessionHandle session, const std::string& room,
                       bool publishing) const;

    IocpServer& server_;
    Authenticator authenticator_;
    RoomAuthorizer room_authorizer_;
    RoomRegistry rooms_;
    mutable std::mutex principals_mutex_;
    std::unordered_map<SessionHandle, std::string, SessionHandleHash> principals_;
    std::atomic<std::uint64_t> server_sequence_{1};
};

}
