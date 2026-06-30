#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace RealtimeEngine {

template <typename T>
class TypedObjectPool {
    static_assert(!std::is_array_v<T>);

    union Slot {
        alignas(T) std::byte storage[sizeof(T)];
        Slot* next;
    };

public:
    explicit TypedObjectPool(std::size_t capacity)
        : capacity_(capacity) {
        if (capacity == 0) {
            throw std::invalid_argument("TypedObjectPool capacity must be non-zero");
        }
        if (capacity > std::numeric_limits<std::size_t>::max() / sizeof(Slot)) {
            throw std::length_error("TypedObjectPool capacity overflows allocation size");
        }

        slots_ = static_cast<Slot*>(
            ::operator new[](capacity * sizeof(Slot), std::align_val_t{alignof(Slot)}));
#ifndef NDEBUG
        allocated_.assign(capacity, false);
#endif
        for (std::size_t i = 0; i + 1 < capacity; ++i) {
            slots_[i].next = &slots_[i + 1];
        }
        slots_[capacity - 1].next = nullptr;
        free_head_ = slots_;
    }

    ~TypedObjectPool() {
        ::operator delete[](slots_, std::align_val_t{alignof(Slot)});
    }

    TypedObjectPool(const TypedObjectPool&) = delete;
    TypedObjectPool& operator=(const TypedObjectPool&) = delete;

    template <typename... Args>
    [[nodiscard]] T* Create(Args&&... args) {
        Slot* slot = nullptr;
        {
            std::lock_guard lock(mutex_);
            if (free_head_ == nullptr) {
                return nullptr;
            }
            slot = free_head_;
            free_head_ = free_head_->next;
#ifndef NDEBUG
            const auto index = static_cast<std::size_t>(slot - slots_);
            if (allocated_[index]) {
                throw std::logic_error("TypedObjectPool free-list corruption");
            }
            allocated_[index] = true;
#endif
        }

        try {
            return std::construct_at(reinterpret_cast<T*>(slot->storage),
                                     std::forward<Args>(args)...);
        } catch (...) {
            std::lock_guard lock(mutex_);
#ifndef NDEBUG
            allocated_[static_cast<std::size_t>(slot - slots_)] = false;
#endif
            slot->next = free_head_;
            free_head_ = slot;
            throw;
        }
    }

    void Destroy(T* object) {
        if (object == nullptr) {
            return;
        }

        auto* bytes = reinterpret_cast<std::byte*>(object);
        auto* begin = reinterpret_cast<std::byte*>(slots_);
        auto* end = begin + capacity_ * sizeof(Slot);
        if (bytes < begin || bytes >= end) {
            throw std::invalid_argument("pointer does not belong to TypedObjectPool");
        }
        const auto offset = static_cast<std::size_t>(bytes - begin);
        if (offset % sizeof(Slot) != offsetof(Slot, storage)) {
            throw std::invalid_argument("pointer is not at a TypedObjectPool block boundary");
        }

        auto* slot = reinterpret_cast<Slot*>(bytes - offsetof(Slot, storage));
        const auto index = static_cast<std::size_t>(slot - slots_);
        {
            std::lock_guard lock(mutex_);
#ifndef NDEBUG
            if (!allocated_[index]) {
                throw std::logic_error("TypedObjectPool double free detected");
            }
            allocated_[index] = false;
#endif
        }

        std::destroy_at(object);
        {
            std::lock_guard lock(mutex_);
            slot->next = free_head_;
            free_head_ = slot;
        }
    }

    [[nodiscard]] std::size_t Capacity() const noexcept { return capacity_; }

private:
    const std::size_t capacity_;
    Slot* slots_ = nullptr;
    Slot* free_head_ = nullptr;
    mutable std::mutex mutex_;
#ifndef NDEBUG
    std::vector<bool> allocated_;
#endif
};

}
