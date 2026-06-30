#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace RealtimeEngine {

using SharedBuffer = std::shared_ptr<const std::vector<std::uint8_t>>;

enum class QueueResult {
    Accepted,
    SessionLimitExceeded,
    GlobalLimitExceeded,
    Closed
};

class GlobalQueueBudget {
public:
    explicit GlobalQueueBudget(std::size_t limit) : limit_(limit) {}

    bool TryReserve(std::size_t bytes) {
        std::lock_guard lock(mutex_);
        if (bytes > limit_ || used_ > limit_ - bytes) {
            return false;
        }
        used_ += bytes;
        return true;
    }

    void Release(std::size_t bytes) {
        std::lock_guard lock(mutex_);
        used_ = bytes > used_ ? 0 : used_ - bytes;
    }

    std::size_t Used() const {
        std::lock_guard lock(mutex_);
        return used_;
    }

private:
    const std::size_t limit_;
    mutable std::mutex mutex_;
    std::size_t used_ = 0;
};

class OutboundQueue {
public:
    OutboundQueue(std::size_t session_limit, GlobalQueueBudget& global_budget)
        : session_limit_(session_limit), global_budget_(global_budget) {}

    QueueResult Push(SharedBuffer buffer) {
        if (!buffer) {
            return QueueResult::Closed;
        }
        const auto bytes = buffer->size();
        std::lock_guard lock(mutex_);
        if (closed_) {
            return QueueResult::Closed;
        }
        if (bytes > session_limit_ || queued_bytes_ > session_limit_ - bytes) {
            return QueueResult::SessionLimitExceeded;
        }
        if (!global_budget_.TryReserve(bytes)) {
            return QueueResult::GlobalLimitExceeded;
        }
        queue_.push_back(std::move(buffer));
        queued_bytes_ += bytes;
        return QueueResult::Accepted;
    }

    SharedBuffer Pop() {
        std::lock_guard lock(mutex_);
        if (queue_.empty()) {
            return {};
        }
        auto value = std::move(queue_.front());
        queue_.pop_front();
        queued_bytes_ -= value->size();
        global_budget_.Release(value->size());
        return value;
    }

    void Close() {
        std::lock_guard lock(mutex_);
        closed_ = true;
        for (const auto& buffer : queue_) {
            global_budget_.Release(buffer->size());
        }
        queue_.clear();
        queued_bytes_ = 0;
    }

    std::size_t QueuedBytes() const {
        std::lock_guard lock(mutex_);
        return queued_bytes_;
    }

private:
    const std::size_t session_limit_;
    GlobalQueueBudget& global_budget_;
    mutable std::mutex mutex_;
    std::deque<SharedBuffer> queue_;
    std::size_t queued_bytes_ = 0;
    bool closed_ = false;
};

}
