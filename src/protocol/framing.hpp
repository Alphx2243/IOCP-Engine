#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <stdexcept>
#include <vector>

namespace RealtimeEngine {

enum class FrameDecodeStatus {
    NeedMoreData,
    MessageReady,
    InvalidLength,
    BufferLimitExceeded
};

class LengthPrefixedAccumulator {
public:
    LengthPrefixedAccumulator(std::size_t max_message_bytes,
                              std::size_t max_buffered_bytes)
        : max_message_bytes_(max_message_bytes),
          max_buffered_bytes_(max_buffered_bytes) {}

    [[nodiscard]] bool Append(std::span<const std::uint8_t> bytes) {
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

    [[nodiscard]] FrameDecodeStatus Next(std::span<const std::uint8_t>& payload) {
        payload = {};
        if (limit_exceeded_) {
            return FrameDecodeStatus::BufferLimitExceeded;
        }
        const auto available = buffer_.size() - cursor_;
        if (available < HeaderSize) {
            return FrameDecodeStatus::NeedMoreData;
        }

        const auto* header = buffer_.data() + cursor_;
        const std::uint32_t length =
            (static_cast<std::uint32_t>(header[0]) << 24U) |
            (static_cast<std::uint32_t>(header[1]) << 16U) |
            (static_cast<std::uint32_t>(header[2]) << 8U) |
            static_cast<std::uint32_t>(header[3]);
        if (length == 0 || length > max_message_bytes_) {
            return FrameDecodeStatus::InvalidLength;
        }
        if (available - HeaderSize < length) {
            return FrameDecodeStatus::NeedMoreData;
        }

        payload = std::span<const std::uint8_t>(
            buffer_.data() + cursor_ + HeaderSize, length);
        cursor_ += HeaderSize + length;
        return FrameDecodeStatus::MessageReady;
    }

    void Reset() {
        buffer_.clear();
        cursor_ = 0;
        limit_exceeded_ = false;
    }

    [[nodiscard]] std::size_t BufferedBytes() const noexcept {
        return buffer_.size() - cursor_;
    }

    static std::array<std::uint8_t, 4> EncodeLength(std::size_t length) {
        if (length > std::numeric_limits<std::uint32_t>::max()) {
            throw std::length_error("frame payload exceeds 32-bit length prefix");
        }
        const auto value = static_cast<std::uint32_t>(length);
        return {
            static_cast<std::uint8_t>((value >> 24U) & 0xffU),
            static_cast<std::uint8_t>((value >> 16U) & 0xffU),
            static_cast<std::uint8_t>((value >> 8U) & 0xffU),
            static_cast<std::uint8_t>(value & 0xffU)
        };
    }

private:
    void CompactIfUseful() {
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

    static constexpr std::size_t HeaderSize = 4;
    std::size_t max_message_bytes_;
    std::size_t max_buffered_bytes_;
    std::vector<std::uint8_t> buffer_;
    std::size_t cursor_ = 0;
    bool limit_exceeded_ = false;
};

}
