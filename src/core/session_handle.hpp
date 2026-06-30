#pragma once

#include <cstdint>
#include <functional>

namespace RealtimeEngine {

struct SessionHandle {
    std::uint64_t id = 0;
    std::uint64_t generation = 0;

    friend bool operator==(const SessionHandle&, const SessionHandle&) = default;
    explicit operator bool() const noexcept { return id != 0; }
};

struct SessionHandleHash {
    std::size_t operator()(const SessionHandle& value) const noexcept {
        const auto first = std::hash<std::uint64_t>{}(value.id);
        const auto second = std::hash<std::uint64_t>{}(value.generation);
        return first ^ (second + 0x9e3779b97f4a7c15ULL + (first << 6U) + (first >> 2U));
    }
};

}
