#pragma once

#include "core/outbound_queue.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>

namespace RealtimeEngine {

class SendCursor {
public:
    SendCursor(std::span<const std::uint8_t> header, SharedBuffer payload)
        : header_size_(header.size()), payload_(std::move(payload)) {
        if (header.size() > header_.size()) {
            throw std::length_error("send header exceeds cursor capacity");
        }
        std::copy(header.begin(), header.end(), header_.begin());
    }

    void Consume(std::size_t bytes) {
        const auto remaining = Remaining();
        sent_ += bytes > remaining ? remaining : bytes;
    }

    [[nodiscard]] std::size_t Remaining() const noexcept {
        return TotalSize() - sent_;
    }

    [[nodiscard]] bool Complete() const noexcept { return Remaining() == 0; }

    [[nodiscard]] std::size_t TotalSize() const noexcept {
        return header_size_ + (payload_ ? payload_->size() : 0);
    }

    [[nodiscard]] std::span<const std::uint8_t> HeaderRemainder() const {
        if (sent_ >= header_size_) {
            return {};
        }
        return std::span(header_).first(header_size_).subspan(sent_);
    }

    [[nodiscard]] std::span<const std::uint8_t> PayloadRemainder() const {
        if (!payload_) {
            return {};
        }
        const auto payload_sent = sent_ > header_size_ ? sent_ - header_size_ : 0;
        return std::span(*payload_).subspan(payload_sent);
    }

private:
    std::array<std::uint8_t, 16> header_{};
    std::size_t header_size_ = 0;
    SharedBuffer payload_;
    std::size_t sent_ = 0;
};

}
