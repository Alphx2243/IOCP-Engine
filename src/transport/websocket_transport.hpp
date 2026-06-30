#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace RealtimeEngine {
/*
    RFC6455 Frame: 
    FRRR : 1 bit each (FIN, RSV1, RSV2, RSV3)
    OPCODE : 4 bits
    MASK : 1 bit
    PAYLOAD LEN : 7 bits (or 7 + 16 bits or 7 + 64 bits)
    MASKING KEY : 0 or 4 [bytes]
    PAYLOAD DATA : (x) [bytes]

*/ 
enum class WsOpcode : std::uint8_t {
    Continuation = 0x0, Text = 0x1, Binary = 0x2, Close = 0x8, Ping = 0x9, Pong = 0xA
};

enum class WsParseStatus {
    NeedMoreData, EventReady, ProtocolError, MessageTooLarge, BufferLimitExceeded
};

enum class WsEventType {
    BinaryMessage, Ping, Pong, Close
};

struct WsEvent {
    WsEventType type = WsEventType::BinaryMessage;
    std::vector<std::uint8_t> payload;
    std::uint16_t close_code = 1000; // 1000 = normal closure
};

enum class HandshakeStatus {
    NeedMoreData, Accepted, Rejected
};

struct HandshakeResult {
    HandshakeStatus status = HandshakeStatus::NeedMoreData;
    std::size_t consumed = 0;
    std::string response, origin, error;
};

class WebSocketDecoder {
public:
    WebSocketDecoder(std::size_t max_message_bytes, std::size_t max_buffered_bytes, bool require_masking = true);

    bool Append(std::span<const std::uint8_t> bytes);
    
    WsParseStatus Next(WsEvent& event, std::string& error);
    
    std::size_t BufferedBytes() const noexcept;
    
    void Reset();
private:
    void CompactIfUseful();
    std::size_t max_message_bytes_;
    std::size_t max_buffered_bytes_;
    bool require_masking_;
    std::vector<std::uint8_t> buffer_;
    std::size_t cursor_ = 0;
    bool fragmented_ = false;
    WsOpcode fragmented_opcode_ = WsOpcode::Continuation;
    std::vector<std::uint8_t> fragmented_payload_;
    bool limit_exceeded_ = false;
};

class WebSocketTransport {
public:
    using OriginValidator = std::function<bool(std::string_view)>;
    
    static HandshakeResult ParseHandshake(
        std::span<const std::uint8_t> bytes,
        std::size_t max_header_bytes,
        const OriginValidator& origin_validator = {});

    static std::array<std::uint8_t, 10> BuildFrameHeader(
        WsOpcode opcode, std::size_t payload_size, std::size_t& header_size);

    static std::vector<std::uint8_t> BuildFrame(
        WsOpcode opcode, std::span<const std::uint8_t> payload);

    static std::string ComputeAcceptKey(std::string_view client_key);

private:
    static void Sha1(std::span<const std::uint8_t> data, std::array<std::uint8_t, 20>& digest); // converts amount of input into exactly 20 bytes (160 bits)
    static std::string Base64Encode(std::span<const std::uint8_t> data);
};
}