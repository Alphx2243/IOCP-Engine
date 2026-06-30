#include "application/protocol_router.hpp"

#include "protocol_generated.h"

#include <chrono>
#include <memory>
#include <string_view>
#include <vector>

namespace RealtimeEngine {
namespace {

constexpr std::uint16_t ProtocolVersion = 1;
constexpr std::size_t MaxIdentifierLength = 160;
constexpr std::size_t MaxDisplayLength = 256;
constexpr std::size_t MaxTextLength = 4096;

std::uint64_t UnixTimeMilliseconds() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

bool ValidString(const flatbuffers::String* value, std::size_t max_length,
                 bool allow_empty = false) {
    return value != nullptr && value->size() <= max_length &&
           (allow_empty || !value->str().empty());
}

bool ValidateMatchInfo(const MatchInfo* value) {
    return value != nullptr &&
           ValidString(value->match_id(), MaxIdentifierLength) &&
           ValidString(value->sport_name(), MaxIdentifierLength) &&
           ValidString(value->team1(), MaxDisplayLength) &&
           ValidString(value->team2(), MaxDisplayLength) &&
           value->team1()->str() != value->team2()->str();
}

bool ValidateMatchScore(const MatchScoreUpdate* value) {
    return value != nullptr &&
           ValidString(value->match_id(), MaxIdentifierLength) &&
           (!value->timeline_event() ||
            ValidString(value->timeline_event(), MaxTextLength, true));
}

bool ValidateFacility(const FacilityUpdate* value) {
    if (value == nullptr ||
        !ValidString(value->facility_id(), MaxIdentifierLength) ||
        value->courts() == nullptr || value->courts()->size() > 1024) {
        return false;
    }
    for (const auto* court : *value->courts()) {
        if (court == nullptr ||
            !ValidString(court->facility_id(), MaxIdentifierLength) ||
            !ValidString(court->court_id(), MaxIdentifierLength) ||
            !ValidString(court->sport_name(), MaxIdentifierLength) ||
            !ValidString(court->court_name(), MaxDisplayLength) ||
            court->facility_id()->str() != value->facility_id()->str()) {
            return false;
        }
    }
    return true;
}

bool ValidateEquipment(const EquipmentUpdate* value) {
    if (value == nullptr ||
        !ValidString(value->facility_id(), MaxIdentifierLength) ||
        value->inventory() == nullptr ||
        value->inventory()->size() > 4096) {
        return false;
    }
    for (const auto* item : *value->inventory()) {
        if (item == nullptr ||
            !ValidString(item->facility_id(), MaxIdentifierLength) ||
            !ValidString(item->equipment_id(), MaxIdentifierLength) ||
            !ValidString(item->item_name(), MaxDisplayLength) ||
            item->facility_id()->str() != value->facility_id()->str() ||
            item->available_count() > item->total_inventory()) {
            return false;
        }
    }
    return true;
}

bool ValidateBooking(const BookingNotification* value) {
    return value != nullptr &&
           ValidString(value->booking_id(), MaxIdentifierLength) &&
           ValidString(value->user_id(), MaxIdentifierLength) &&
           ValidString(value->resource_id(), MaxIdentifierLength) &&
           (!value->user_display_name() ||
            ValidString(value->user_display_name(), MaxDisplayLength, true)) &&
           value->start_time_ms() < value->end_time_ms();
}

bool ValidateAlert(const SystemAlert* value) {
    return value != nullptr &&
           ValidString(value->message(), MaxTextLength);
}

bool ValidateDomain(const Publish* publish) {
    if (publish == nullptr ||
        !ValidString(publish->room(), MaxIdentifierLength) ||
        publish->message() == nullptr) {
        return false;
    }
    switch (publish->message_type()) {
        case DomainPayload::MatchInfo:
            return ValidateMatchInfo(publish->message_as_MatchInfo());
        case DomainPayload::MatchScoreUpdate:
            return ValidateMatchScore(
                publish->message_as_MatchScoreUpdate());
        case DomainPayload::FacilityUpdate:
            return ValidateFacility(publish->message_as_FacilityUpdate());
        case DomainPayload::EquipmentUpdate:
            return ValidateEquipment(publish->message_as_EquipmentUpdate());
        case DomainPayload::BookingNotification:
            return ValidateBooking(
                publish->message_as_BookingNotification());
        case DomainPayload::SystemAlert:
            return ValidateAlert(publish->message_as_SystemAlert());
        case DomainPayload::NONE:
            return false;
    }
    return false;
}

SharedBuffer Finish(flatbuffers::FlatBufferBuilder& builder,
                    flatbuffers::Offset<NetworkMessage> root) {
    FinishNetworkMessageBuffer(builder, root);
    return std::make_shared<const std::vector<std::uint8_t>>(
        builder.GetBufferPointer(),
        builder.GetBufferPointer() + builder.GetSize());
}

std::string RoomResultReason(RoomResult result) {
    switch (result) {
        case RoomResult::Ok: return {};
        case RoomResult::InvalidRoom: return "invalid room";
        case RoomResult::Unauthorized: return "room authorization denied";
        case RoomResult::AlreadySubscribed: return "already subscribed";
        case RoomResult::NotSubscribed: return "not subscribed";
    }
    return "room operation failed";
}

}

ProtocolRouter::ProtocolRouter(IocpServer& server,
                               Authenticator authenticator,
                               RoomAuthorizer room_authorizer)
    : server_(server),
      authenticator_(std::move(authenticator)),
      room_authorizer_(std::move(room_authorizer)),
      rooms_([this](SessionHandle session, const std::string& room,
                    bool publishing) {
          return AuthorizeRoom(session, room, publishing);
      }) {}

MessageDisposition ProtocolRouter::Handle(
    SessionHandle session, std::span<const std::uint8_t> payload) {
    flatbuffers::Verifier verifier(payload.data(), payload.size());
    if (!VerifyNetworkMessageBuffer(verifier)) {
        return MessageDisposition::Malformed;
    }
    const auto* message = GetNetworkMessage(payload.data());
    const auto request_id = message->request_id();

    const auto send_error = [&](ErrorCode code, std::string_view text) {
        flatbuffers::FlatBufferBuilder builder(256);
        const auto text_offset = builder.CreateString(text);
        const auto error =
            CreateErrorResponse(builder, code, text_offset, request_id);
        const auto root = CreateNetworkMessage(
            builder, ProtocolVersion, 0, server_sequence_.fetch_add(1),
            UnixTimeMilliseconds(), Payload::ErrorResponse, error.Union());
        server_.Send(session, Finish(builder, root));
    };
    const auto send_ack = [&](std::string_view room,
                              SubscriptionOperation operation,
                              RoomResult result) {
        flatbuffers::FlatBufferBuilder builder(256);
        const auto room_offset = builder.CreateString(room);
        const auto reason = RoomResultReason(result);
        const auto reason_offset = reason.empty()
            ? flatbuffers::Offset<flatbuffers::String>{}
            : builder.CreateString(reason);
        const auto ack = CreateSubscriptionAck(
            builder, room_offset, operation, result == RoomResult::Ok,
            reason_offset);
        const auto root = CreateNetworkMessage(
            builder, ProtocolVersion, request_id,
            server_sequence_.fetch_add(1), UnixTimeMilliseconds(),
            Payload::SubscriptionAck, ack.Union());
        server_.Send(session, Finish(builder, root));
    };

    if (message->protocol_version() != ProtocolVersion) {
        send_error(ErrorCode::UNSUPPORTED_VERSION,
                   "supported protocol_version is 1");
        return MessageDisposition::Accepted;
    }
    if (message->payload_type() == Payload::NONE ||
        message->payload() == nullptr) {
        send_error(ErrorCode::MALFORMED_MESSAGE, "payload is required");
        return MessageDisposition::Malformed;
    }

    switch (message->payload_type()) {
        case Payload::Authenticate: {
            const auto* authenticate = message->payload_as_Authenticate();
            if (authenticate == nullptr ||
                !ValidString(authenticate->token(), MaxTextLength)) {
                send_error(ErrorCode::MALFORMED_MESSAGE,
                           "authentication token is required");
                return MessageDisposition::Malformed;
            }
            const auto principal = authenticator_
                ? authenticator_(std::string_view(
                      authenticate->token()->c_str(),
                      authenticate->token()->size()))
                : std::optional<std::string>{};
            if (principal) {
                std::lock_guard lock(principals_mutex_);
                principals_[session] = *principal;
            }
            flatbuffers::FlatBufferBuilder builder(256);
            const auto principal_offset = principal
                ? builder.CreateString(*principal)
                : flatbuffers::Offset<flatbuffers::String>{};
            const auto reason_offset = principal
                ? flatbuffers::Offset<flatbuffers::String>{}
                : builder.CreateString("authentication rejected");
            const auto result = CreateAuthenticationResult(
                builder, principal.has_value(), principal_offset,
                reason_offset);
            const auto root = CreateNetworkMessage(
                builder, ProtocolVersion, request_id,
                server_sequence_.fetch_add(1), UnixTimeMilliseconds(),
                Payload::AuthenticationResult, result.Union());
            server_.Send(session, Finish(builder, root));
            return MessageDisposition::Accepted;
        }
        case Payload::Ping: {
            const auto* ping = message->payload_as_Ping();
            if (ping == nullptr) return MessageDisposition::Malformed;
            flatbuffers::FlatBufferBuilder builder(128);
            const auto pong = CreatePong(builder, ping->timestamp_ms());
            const auto root = CreateNetworkMessage(
                builder, ProtocolVersion, request_id,
                server_sequence_.fetch_add(1), UnixTimeMilliseconds(),
                Payload::Pong, pong.Union());
            server_.Send(session, Finish(builder, root));
            return MessageDisposition::Accepted;
        }
        case Payload::Pong:
            return MessageDisposition::Accepted;
        case Payload::JoinRoom: {
            if (!IsAuthenticated(session)) {
                send_error(ErrorCode::UNAUTHORIZED, "authenticate before joining rooms");
                return MessageDisposition::Accepted;
            }
            const auto* join = message->payload_as_JoinRoom();
            if (join == nullptr || !ValidString(join->room(), MaxIdentifierLength)) {
                send_error(ErrorCode::INVALID_ROOM, "room is required");
                return MessageDisposition::Malformed;
            }
            const auto room = join->room()->str();
            send_ack(room, SubscriptionOperation::JOIN, rooms_.Join(session, room));
            return MessageDisposition::Accepted;
        }
        case Payload::LeaveRoom: {
            const auto* leave = message->payload_as_LeaveRoom();
            if (leave == nullptr || !ValidString(leave->room(), MaxIdentifierLength)) {
                send_error(ErrorCode::INVALID_ROOM, "room is required");
                return MessageDisposition::Malformed;
            }
            const auto room = leave->room()->str();
            send_ack(room, SubscriptionOperation::LEAVE, rooms_.Leave(session, room));
            return MessageDisposition::Accepted;
        }
        case Payload::Publish: {
            if (!IsAuthenticated(session)) {
                send_error(ErrorCode::UNAUTHORIZED, "authenticate before publishing");
                return MessageDisposition::Accepted;
            }
            const auto* publish = message->payload_as_Publish();
            if (!ValidateDomain(publish)) {
                send_error(ErrorCode::MALFORMED_MESSAGE, "published domain payload is invalid");
                return MessageDisposition::Malformed;
            }
            const auto room = publish->room()->str();
            const auto authorization = rooms_.AuthorizePublish(session, room);
            if (authorization != RoomResult::Ok) {
                send_error(
                    authorization == RoomResult::Unauthorized ? ErrorCode::FORBIDDEN : ErrorCode::NOT_SUBSCRIBED,
                    RoomResultReason(authorization));
                return MessageDisposition::Accepted;
            }
            auto immutable = std::make_shared<const std::vector<std::uint8_t>>(payload.begin(), payload.end());
            for (const auto recipient : rooms_.Snapshot(room)) server_.Send(recipient, immutable);
            return MessageDisposition::Accepted;
        }
        case Payload::MatchInfo:
        case Payload::MatchScoreUpdate:
        case Payload::FacilityUpdate:
        case Payload::EquipmentUpdate:
        case Payload::BookingNotification:
        case Payload::SystemAlert:
        case Payload::AuthenticationResult:
        case Payload::SubscriptionAck:
        case Payload::ErrorResponse:
            send_error(ErrorCode::FORBIDDEN, "payload type is server-to-client only");
            return MessageDisposition::Accepted;
        case Payload::NONE:
            break;
    }
    return MessageDisposition::Malformed;
}

void ProtocolRouter::OnDisconnect(SessionHandle session) {
    rooms_.RemoveSession(session);
    std::lock_guard lock(principals_mutex_);
    principals_.erase(session);
}

bool ProtocolRouter::IsAuthenticated(SessionHandle session) const {
    return Principal(session).has_value();
}

std::optional<std::string> ProtocolRouter::Principal(
    SessionHandle session) const {
    std::lock_guard lock(principals_mutex_);
    const auto it = principals_.find(session);
    if (it == principals_.end()) return std::nullopt;
    return it->second;
}

bool ProtocolRouter::AuthorizeRoom(SessionHandle session, const std::string& room, bool publishing) const {
    const auto principal = Principal(session);
    if (!principal) return false;
    if (room_authorizer_) return room_authorizer_(*principal, room, publishing);
    constexpr std::string_view BookingPrefix = "booking:user:";
    if (room.starts_with(BookingPrefix)) return room.substr(BookingPrefix.size()) == *principal;
    return true;
}

}
