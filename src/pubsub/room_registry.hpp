#pragma once

#include "core/session_handle.hpp"

#include <algorithm>
#include <functional>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace RealtimeEngine {

enum class RoomResult {
    Ok,
    InvalidRoom,
    Unauthorized,
    AlreadySubscribed,
    NotSubscribed
};

class RoomRegistry {
public:
    using Authorizer = std::function<bool(SessionHandle, const std::string&, bool)>;

    explicit RoomRegistry(Authorizer authorizer = {})
        : authorizer_(std::move(authorizer)) {}

    RoomResult Join(SessionHandle session, const std::string& room) {
        if (!IsValidRoom(room)) {
            return RoomResult::InvalidRoom;
        }
        if (authorizer_ && !authorizer_(session, room, false)) {
            return RoomResult::Unauthorized;
        }

        std::lock_guard lock(mutex_);
        auto& rooms = session_to_rooms_[session];
        if (!rooms.insert(room).second) {
            return RoomResult::AlreadySubscribed;
        }
        room_to_sessions_[room].insert(session);
        return RoomResult::Ok;
    }

    RoomResult Leave(SessionHandle session, const std::string& room) {
        std::lock_guard lock(mutex_);
        auto session_it = session_to_rooms_.find(session);
        if (session_it == session_to_rooms_.end() ||
            session_it->second.erase(room) == 0) {
            return RoomResult::NotSubscribed;
        }
        if (session_it->second.empty()) {
            session_to_rooms_.erase(session_it);
        }

        auto room_it = room_to_sessions_.find(room);
        if (room_it != room_to_sessions_.end()) {
            room_it->second.erase(session);
            if (room_it->second.empty()) {
                room_to_sessions_.erase(room_it);
            }
        }
        return RoomResult::Ok;
    }

    RoomResult AuthorizePublish(SessionHandle session, const std::string& room) const {
        if (!IsValidRoom(room)) {
            return RoomResult::InvalidRoom;
        }
        if (authorizer_ && !authorizer_(session, room, true)) {
            return RoomResult::Unauthorized;
        }
        std::lock_guard lock(mutex_);
        const auto session_it = session_to_rooms_.find(session);
        if (session_it == session_to_rooms_.end() ||
            session_it->second.count(room) == 0) {
            return RoomResult::NotSubscribed;
        }
        return RoomResult::Ok;
    }

    std::vector<SessionHandle> Snapshot(const std::string& room) const {
        std::lock_guard lock(mutex_);
        const auto it = room_to_sessions_.find(room);
        if (it == room_to_sessions_.end()) {
            return {};
        }
        return {it->second.begin(), it->second.end()};
    }

    void RemoveSession(SessionHandle session) {
        std::lock_guard lock(mutex_);
        auto session_it = session_to_rooms_.find(session);
        if (session_it == session_to_rooms_.end()) {
            return;
        }
        for (const auto& room : session_it->second) {
            auto room_it = room_to_sessions_.find(room);
            if (room_it == room_to_sessions_.end()) {
                continue;
            }
            room_it->second.erase(session);
            if (room_it->second.empty()) {
                room_to_sessions_.erase(room_it);
            }
        }
        session_to_rooms_.erase(session_it);
    }

    static bool IsValidRoom(const std::string& room) {
        if (room.empty() || room.size() > 160) {
            return false;
        }
        static constexpr const char* prefixes[] = {
            "match:", "facility:", "court:", "equipment:", "booking:user:"
        };
        const bool valid_prefix = std::any_of(
            std::begin(prefixes), std::end(prefixes),
            [&room](const char* prefix) { return room.starts_with(prefix); });
        if (!valid_prefix) {
            return false;
        }
        return std::all_of(room.begin(), room.end(), [](unsigned char value) {
            return (value >= 'a' && value <= 'z') ||
                   (value >= 'A' && value <= 'Z') ||
                   (value >= '0' && value <= '9') ||
                   value == ':' || value == '-' || value == '_' || value == '.';
        });
    }

private:
    Authorizer authorizer_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string,
        std::unordered_set<SessionHandle, SessionHandleHash>> room_to_sessions_;
    std::unordered_map<SessionHandle, std::unordered_set<std::string>,
        SessionHandleHash> session_to_rooms_;
};

}
