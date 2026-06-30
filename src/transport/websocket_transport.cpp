#include "transport/websocket_transport.hpp"
#include <algorithm>
#include <array>
#include <cctype>
#include <cstring>
#include <limits>
#include <sstream>
#include <unordered_map>

namespace RealtimeEngine {
namespace {

std::string Trim(std::string_view value) {
    while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) value.remove_prefix(1);
    while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) value.remove_suffix(1);
    return std::string(value);
}

std::string Lower(std::string_view value) {
    std::string result(value);
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char ch) {
                       return static_cast<char>(std::tolower(ch));
                   });
    return result;
}

bool ContainsToken(std::string_view value, std::string_view expected) {
    while (!value.empty()) {
        const auto separator = value.find(',');
        const auto token = Trim(value.substr(0, separator));
        if (Lower(token) == Lower(expected)) return true;
        if (separator == std::string_view::npos) break;
        value.remove_prefix(separator + 1);
    }
    return false;
}

int Base64Value(unsigned char value) {
    if (value >= 'A' && value <= 'Z') return value - 'A';
    if (value >= 'a' && value <= 'z') return value - 'a' + 26;
    if (value >= '0' && value <= '9') return value - '0' + 52;
    if (value == '+') return 62;
    if (value == '/') return 63;
    return -1;
}

bool IsValidWebSocketKey(std::string_view key) {
    if (key.size() != 24 || key[22] != '=' || key[23] != '=') return false;
    std::size_t decoded = 0;
    int bits = 0;
    std::uint32_t accumulator = 0;
    for (const unsigned char value : key) {
        if (value == '=') break;
        const int digit = Base64Value(value);
        if (digit < 0) return false;
        accumulator = (accumulator << 6U) | static_cast<std::uint32_t>(digit);
        bits += 6;
        if (bits >= 8) bits -= 8, ++decoded;
    }
    return decoded == 16;
}

bool IsControlOpcode(WsOpcode opcode) {
    return opcode == WsOpcode::Close || opcode == WsOpcode::Ping || opcode == WsOpcode::Pong;
}
bool IsKnownOpcode(WsOpcode opcode) {
    switch (opcode) {
        case WsOpcode::Continuation:
        case WsOpcode::Text:
        case WsOpcode::Binary:
        case WsOpcode::Close:
        case WsOpcode::Ping:
        case WsOpcode::Pong:
            return true;
    }
    return false;
}

bool IsValidCloseCode(std::uint16_t code) {
    if (code < 1000 || code >= 5000) return false;
    if (code == 1004 || code == 1005 || code == 1006 || code == 1015) return false;
    return !(code >= 1016 && code <= 2999);
}

bool IsValidUtf8(std::span<const std::uint8_t> bytes) {
    std::size_t index = 0;
    while (index < bytes.size()) {
        const auto first = bytes[index++];
        if (first <= 0x7f) continue;
        int trailing = 0;
        std::uint32_t codepoint = 0;
        if ((first & 0xe0U) == 0xc0U) trailing = 1, codepoint = first & 0x1fU;
        else if ((first & 0xf0U) == 0xe0U) trailing = 2, codepoint = first & 0x0fU;
        else if ((first & 0xf8U) == 0xf0U) trailing = 3, codepoint = first & 0x07U;
        else return false;
        if (index + static_cast<std::size_t>(trailing) > bytes.size()) return false;
        for (int i = 0; i < trailing; ++i) {
            const auto next = bytes[index++];
            if ((next & 0xc0U) != 0x80U) return false;
            codepoint = (codepoint << 6U) | (next & 0x3fU);
        }
        if ((trailing == 1 && codepoint < 0x80U) ||
            (trailing == 2 && codepoint < 0x800U) ||
            (trailing == 3 && codepoint < 0x10000U) ||
            codepoint > 0x10ffffU || (codepoint >= 0xd800U && codepoint <= 0xdfffU)) return false;
    }
    return true;
}

std::uint32_t RotateLeft(std::uint32_t value, unsigned count) {
    return (value << count) | (value >> (32U - count));
}

}

WebSocketDecoder::WebSocketDecoder(std::size_t max_message_bytes,
                                   std::size_t max_buffered_bytes,
                                   bool require_masking)
    : max_message_bytes_(max_message_bytes),
      max_buffered_bytes_(max_buffered_bytes),
      require_masking_(require_masking) {}

bool WebSocketDecoder::Append(std::span<const std::uint8_t> bytes) {
    CompactIfUseful();
    const auto available = buffer_.size() - cursor_;
    if (bytes.size() > max_buffered_bytes_ ||
        available > max_buffered_bytes_ - bytes.size()) {
        limit_exceeded_ = true;
        return false;
    }
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
    return true;
}

WsParseStatus WebSocketDecoder::Next(WsEvent& event, std::string& error) {
    event = {};
    error.clear();
    if (limit_exceeded_) {
        error = "WebSocket receive buffer limit exceeded";
        return WsParseStatus::BufferLimitExceeded;
    }

    const auto available = buffer_.size() - cursor_;
    if (available < 2) {
        return WsParseStatus::NeedMoreData;
    }
    const auto* data = buffer_.data() + cursor_;
    const bool fin = (data[0] & 0x80U) != 0;
    const bool rsv_set = (data[0] & 0x70U) != 0;
    const auto opcode = static_cast<WsOpcode>(data[0] & 0x0fU);
    const bool masked = (data[1] & 0x80U) != 0;
    const auto length_code = static_cast<std::uint8_t>(data[1] & 0x7fU);
    if (rsv_set || !IsKnownOpcode(opcode)) {
        error = "RSV bits or opcode are invalid";
        return WsParseStatus::ProtocolError;
    }
    if (require_masking_ && !masked) {
        error = "client WebSocket frames must be masked";
        return WsParseStatus::ProtocolError;
    }
    if (!require_masking_ && masked) {
        error = "server WebSocket frames must not be masked";
        return WsParseStatus::ProtocolError;
    }

    std::size_t header_size = 2;
    std::uint64_t payload_size = length_code;
    if (length_code == 126) {
        if (available < 4) {
            return WsParseStatus::NeedMoreData;
        }
        payload_size = (static_cast<std::uint64_t>(data[2]) << 8U) | data[3];
        header_size = 4;
        if (payload_size < 126) {
            error = "non-canonical 16-bit WebSocket length";
            return WsParseStatus::ProtocolError;
        }
    } else if (length_code == 127) {
        if (available < 10) {
            return WsParseStatus::NeedMoreData;
        }
        if ((data[2] & 0x80U) != 0) {
            error = "WebSocket 64-bit length has reserved high bit set";
            return WsParseStatus::ProtocolError;
        }
        payload_size = 0;
        for (int index = 0; index < 8; ++index) {
            payload_size = (payload_size << 8U) | data[2 + index];
        }
        header_size = 10;
        if (payload_size <= 65535) {
            error = "non-canonical 64-bit WebSocket length";
            return WsParseStatus::ProtocolError;
        }
    }

    if (IsControlOpcode(opcode) && (!fin || payload_size > 125)) {
        error = "fragmented or oversized WebSocket control frame";
        return WsParseStatus::ProtocolError;
    }
    if (payload_size > max_message_bytes_) {
        error = "WebSocket message exceeds configured limit";
        return WsParseStatus::MessageTooLarge;
    }
    if (masked) {
        header_size += 4;
    }
    if (payload_size > std::numeric_limits<std::size_t>::max() - header_size) {
        error = "WebSocket frame length overflows address space";
        return WsParseStatus::ProtocolError;
    }
    const auto total_size = header_size + static_cast<std::size_t>(payload_size);
    if (available < total_size) {
        return WsParseStatus::NeedMoreData;
    }

    const std::uint8_t* mask = masked ? data + header_size - 4 : nullptr;
    std::vector<std::uint8_t> payload(static_cast<std::size_t>(payload_size));
    const auto* source = data + header_size;
    for (std::size_t index = 0; index < payload.size(); ++index) {
        payload[index] = masked
            ? static_cast<std::uint8_t>(source[index] ^ mask[index % 4])
            : source[index];
    }
    cursor_ += total_size;

    if (opcode == WsOpcode::Text) {
        error = "text messages are not supported; use binary FlatBuffers";
        return WsParseStatus::ProtocolError;
    }
    if (opcode == WsOpcode::Continuation) {
        if (!fragmented_) {
            error = "unexpected WebSocket continuation frame";
            return WsParseStatus::ProtocolError;
        }
        if (payload.size() > max_message_bytes_ - fragmented_payload_.size()) {
            error = "assembled WebSocket message exceeds configured limit";
            return WsParseStatus::MessageTooLarge;
        }
        fragmented_payload_.insert(fragmented_payload_.end(),
                                   payload.begin(), payload.end());
        if (!fin) {
            return Next(event, error);
        }
        fragmented_ = false;
        if (fragmented_opcode_ != WsOpcode::Binary) {
            error = "text messages are not supported";
            fragmented_payload_.clear();
            return WsParseStatus::ProtocolError;
        }
        event.type = WsEventType::BinaryMessage;
        event.payload = std::move(fragmented_payload_);
        fragmented_payload_.clear();
        return WsParseStatus::EventReady;
    }
    if (opcode == WsOpcode::Binary && !fin) {
        if (fragmented_) {
            error = "new data frame received during fragmented message";
            return WsParseStatus::ProtocolError;
        }
        fragmented_ = true;
        fragmented_opcode_ = opcode;
        fragmented_payload_ = std::move(payload);
        return Next(event, error);
    }
    if (opcode == WsOpcode::Binary) {
        if (fragmented_) {
            error = "new data frame received during fragmented message";
            return WsParseStatus::ProtocolError;
        }
        event.type = WsEventType::BinaryMessage;
        event.payload = std::move(payload);
        return WsParseStatus::EventReady;
    }
    if (opcode == WsOpcode::Ping) {
        event.type = WsEventType::Ping;
        event.payload = std::move(payload);
        return WsParseStatus::EventReady;
    }
    if (opcode == WsOpcode::Pong) {
        event.type = WsEventType::Pong;
        event.payload = std::move(payload);
        return WsParseStatus::EventReady;
    }

    if (payload.size() == 1) {
        error = "WebSocket close payload cannot contain one byte";
        return WsParseStatus::ProtocolError;
    }
    event.type = WsEventType::Close;
    event.payload = std::move(payload);
    if (event.payload.size() >= 2) {
        event.close_code =
            (static_cast<std::uint16_t>(event.payload[0]) << 8U) |
            event.payload[1];
        if (!IsValidCloseCode(event.close_code) ||
            !IsValidUtf8(std::span(event.payload).subspan(2))) {
            error = "WebSocket close code or reason is invalid";
            return WsParseStatus::ProtocolError;
        }
    }
    return WsParseStatus::EventReady;
}

std::size_t WebSocketDecoder::BufferedBytes() const noexcept {
    return buffer_.size() - cursor_;
}

void WebSocketDecoder::Reset() {
    buffer_.clear();
    cursor_ = 0;
    fragmented_ = false;
    fragmented_payload_.clear();
    limit_exceeded_ = false;
}

void WebSocketDecoder::CompactIfUseful() {
    if (cursor_ == 0) {
        return;
    }
    if (cursor_ == buffer_.size()) {
        buffer_.clear();
        cursor_ = 0;
        return;
    }
    if (cursor_ >= 4096 && cursor_ * 2 >= buffer_.size()) {
        std::memmove(buffer_.data(), buffer_.data() + cursor_,
                     buffer_.size() - cursor_);
        buffer_.resize(buffer_.size() - cursor_);
        cursor_ = 0;
    }
}

HandshakeResult WebSocketTransport::ParseHandshake(
    std::span<const std::uint8_t> bytes,
    std::size_t max_header_bytes,
    const OriginValidator& origin_validator) {
    HandshakeResult result;
    if (bytes.size() > max_header_bytes) {
        result.status = HandshakeStatus::Rejected;
        result.error = "HTTP upgrade headers exceed configured limit";
        result.response = "HTTP/1.1 431 Request Header Fields Too Large\r\n"
                          "Connection: close\r\n\r\n";
        return result;
    }

    const std::string_view request(
        reinterpret_cast<const char*>(bytes.data()), bytes.size());
    const auto header_end = request.find("\r\n\r\n");
    if (header_end == std::string_view::npos) {
        return result;
    }
    result.consumed = header_end + 4;

    const auto request_line_end = request.find("\r\n");
    if (request_line_end == std::string_view::npos) {
        result.status = HandshakeStatus::Rejected;
        result.error = "missing HTTP request line";
        result.response = "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
        return result;
    }
    const auto request_line = request.substr(0, request_line_end);
    const auto first_space = request_line.find(' ');
    const auto second_space = request_line.find(' ', first_space + 1);
    if (first_space == std::string_view::npos ||
        second_space == std::string_view::npos ||
        request_line.substr(0, first_space) != "GET" ||
        request_line.substr(second_space + 1) != "HTTP/1.1") {
        result.status = HandshakeStatus::Rejected;
        result.error = "WebSocket upgrade requires GET over HTTP/1.1";
        result.response = "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
        return result;
    }

    std::unordered_map<std::string, std::string> headers;
    std::size_t position = request_line_end + 2;
    while (position < header_end) {
        const auto line_end = request.find("\r\n", position);
        const auto line = request.substr(position, line_end - position);
        const auto colon = line.find(':');
        if (colon == std::string_view::npos) {
            result.status = HandshakeStatus::Rejected;
            result.error = "malformed HTTP header";
            result.response = "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
            return result;
        }
        const auto name = Lower(Trim(line.substr(0, colon)));
        if (headers.contains(name)) {
            result.status = HandshakeStatus::Rejected;
            result.error = "duplicate HTTP header";
            result.response = "HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n";
            return result;
        }
        headers.emplace(name, Trim(line.substr(colon + 1)));
        position = line_end + 2;
    }

    const auto upgrade = headers.find("upgrade");
    const auto connection = headers.find("connection");
    const auto key = headers.find("sec-websocket-key");
    const auto version = headers.find("sec-websocket-version");
    if (upgrade == headers.end() || Lower(upgrade->second) != "websocket" ||
        connection == headers.end() ||
        !ContainsToken(connection->second, "upgrade") ||
        key == headers.end() || !IsValidWebSocketKey(key->second) ||
        version == headers.end() || version->second != "13") {
        result.status = HandshakeStatus::Rejected;
        result.error = "required WebSocket upgrade headers are missing or invalid";
        result.response = "HTTP/1.1 400 Bad Request\r\n" "Sec-WebSocket-Version: 13\r\n" "Connection: close\r\n\r\n";
        return result;
    }

    const auto origin = headers.find("origin");
    if (origin != headers.end()) result.origin = origin->second;
    if (origin_validator && !origin_validator(result.origin)) {
        result.status = HandshakeStatus::Rejected, result.error = "WebSocket Origin is not allowed";
        result.response = "HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n";
        return result;
    }

    result.status = HandshakeStatus::Accepted;
    result.response = "HTTP/1.1 101 Switching Protocols\r\n" "Upgrade: websocket\r\n"
                      "Connection: Upgrade\r\n" "Sec-WebSocket-Accept: " + ComputeAcceptKey(key->second) + "\r\n\r\n";
    return result;
}

std::array<std::uint8_t, 10> WebSocketTransport::BuildFrameHeader(
    WsOpcode opcode, std::size_t payload_size, std::size_t& header_size) {
    std::array<std::uint8_t, 10> header{};
    header[0] = static_cast<std::uint8_t>(0x80U | static_cast<std::uint8_t>(opcode));
    if (payload_size <= 125) {
        header[1] = static_cast<std::uint8_t>(payload_size), header_size = 2;
    } 
    else if (payload_size <= 65535) {
        header[1] = 126, header[2] = static_cast<std::uint8_t>((payload_size >> 8U) & 0xffU);
        header[3] = static_cast<std::uint8_t>(payload_size & 0xffU), header_size = 4;
    } 
    else {
        header[1] = 127;
        const auto size = static_cast<std::uint64_t>(payload_size);
        for (int index = 0; index < 8; ++index) {
            header[9 - index] = static_cast<std::uint8_t>((size >> (index * 8U)) & 0xffU);
        }
        header_size = 10;
    }
    return header;
}

std::vector<std::uint8_t> WebSocketTransport::BuildFrame(
    WsOpcode opcode, std::span<const std::uint8_t> payload) {
    std::size_t header_size = 0;
    const auto header = BuildFrameHeader(opcode, payload.size(), header_size);
    std::vector<std::uint8_t> frame;
    frame.reserve(header_size + payload.size());
    frame.insert(frame.end(), header.begin(), header.begin() + header_size);
    frame.insert(frame.end(), payload.begin(), payload.end());
    return frame;
}

std::string WebSocketTransport::ComputeAcceptKey(std::string_view client_key) {
    constexpr std::string_view Guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    std::string input(client_key);
    input.append(Guid);
    std::array<std::uint8_t, 20> digest{};
    Sha1(std::span(reinterpret_cast<const std::uint8_t*>(input.data()), input.size()), digest);
    return Base64Encode(digest);
}
void WebSocketTransport::Sha1(std::span<const std::uint8_t> data, std::array<std::uint8_t, 20>& digest) {
    std::uint32_t h0 = 0x67452301U, h1 = 0xefcdab89U, h2 = 0x98badcfeU;
    std::uint32_t h3 = 0x10325476U, h4 = 0xc3d2e1f0U;

    const auto bit_length = static_cast<std::uint64_t>(data.size()) * 8U;
    auto padded_size = data.size() + 1;
    while (padded_size % 64 != 56) ++padded_size;
    
    padded_size += 8;
    std::vector<std::uint8_t> padded(padded_size);
    std::copy(data.begin(), data.end(), padded.begin());
    padded[data.size()] = 0x80;
    for (int index = 0; index < 8; ++index) {
        padded[padded_size - 1 - index] = static_cast<std::uint8_t>(bit_length >> (index * 8U));
    }
    for (std::size_t offset = 0; offset < padded.size(); offset += 64) {
        std::uint32_t words[80]{};
        for (int index = 0; index < 16; ++index) {
            const auto base = offset + static_cast<std::size_t>(index) * 4;
            words[index] = (static_cast<std::uint32_t>(padded[base]) << 24U) |
                (static_cast<std::uint32_t>(padded[base + 1]) << 16U) |
                (static_cast<std::uint32_t>(padded[base + 2]) << 8U) |
                padded[base + 3];
        }
        for (int index = 16; index < 80; ++index) {
            words[index] = RotateLeft(words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16], 1);
        }
        auto a = h0;
        auto b = h1;
        auto c = h2;
        auto d = h3;
        auto e = h4;
        for (int index = 0; index < 80; ++index) {
            std::uint32_t function = 0, constant = 0;
            if (index < 20) {
                function = (b & c) | ((~b) & d), constant = 0x5a827999U;
            } 
            else if (index < 40) {
                function = b ^ c ^ d, constant = 0x6ed9eba1U;
            } 
            else if (index < 60) {
                function = (b & c) | (b & d) | (c & d), constant = 0x8f1bbcdcU;
            }
            else function = b ^ c ^ d, constant = 0xca62c1d6U;
            
            const auto temporary = RotateLeft(a, 5) + function + e + constant + words[index];
            e = d, d = c, c = RotateLeft(b, 30), b = a, a = temporary;
        }
        h0 += a, h1 += b, h2 += c, h3 += d, h4 += e;
    }

    const std::uint32_t values[] = {h0, h1, h2, h3, h4};
    for (int index = 0; index < 5; ++index) {
        digest[index * 4] = static_cast<std::uint8_t>(values[index] >> 24U);
        digest[index * 4 + 1] = static_cast<std::uint8_t>(values[index] >> 16U);
        digest[index * 4 + 2] = static_cast<std::uint8_t>(values[index] >> 8U);
        digest[index * 4 + 3] = static_cast<std::uint8_t>(values[index]);
    }
}

std::string WebSocketTransport::Base64Encode(
    std::span<const std::uint8_t> data) {
    constexpr char Alphabet[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string result;
    result.reserve(4 * ((data.size() + 2) / 3));
    for (std::size_t index = 0; index < data.size(); index += 3) {
        std::uint32_t value = static_cast<std::uint32_t>(data[index]) << 16U;
        if (index + 1 < data.size()) value |= static_cast<std::uint32_t>(data[index + 1]) << 8U;
        if (index + 2 < data.size()) value |= data[index + 2];
        result.push_back(Alphabet[(value >> 18U) & 0x3fU]);
        result.push_back(Alphabet[(value >> 12U) & 0x3fU]);
        result.push_back(index + 1 < data.size() ? Alphabet[(value >> 6U) & 0x3fU] : '=');
        result.push_back(index + 2 < data.size() ? Alphabet[value & 0x3fU] : '=');
    }
    return result;
}
}