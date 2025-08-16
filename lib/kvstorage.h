#pragma once
#include <iostream>
#include <ctype.h>
#include <optional>
#include <map>
#include <unordered_map>
#include <span>
#include <vector>
#include <queue>

template<typename Clock>
class KVStorage final {
public:
    explicit KVStorage(std::span<std::tuple<std::string, std::string, uint32_t>> entries, Clock clock = Clock())
    :
    clock_(std::move(clock))
    {
        map_.reserve(entries.size());
        for (const auto& i : entries) {
            const auto& key = std::get<0>(i);
            const auto& value = std::get<1>(i);
            uint32_t ttl = std::get<2>(i);
            set(key, value, ttl);
        }
    }

    ~KVStorage() = default;
    
    void set(std::string key, std::string value, uint32_t ttl) {
        const uint64_t now = now_sec();
        const uint64_t expired = (ttl == 0) ? k_infinite : now + static_cast<uint64_t>(ttl);

        auto it = map_.find(key);
        if (it == map_.end()) {
            Entry e;
            e.value_ = std::move(value);
            e.expired_at = expired;
            e.generation = 1;
            auto [ins_it, _] = map_.emplace(std::move(key), std::move(e));
            sorted_.emplace(ins_it->first, 0);
            if (ttl != 0) {
                push_to_heap(ins_it->first, ins_it->second);
            }
        } else {
            it->second.value_ = std::move(value);
            it->second.expired_at = expired;
            ++it->second.generation;
            if (ttl != 0) {
                push_to_heap(it->first, it->second);
            }
        }
    }

    bool remove(std::string_view key) {
        auto it = map_.find(std::string(key));
        if (it == map_.end()) {
            return false;
        }
        sorted_.erase(it->first);
        map_.erase(it);
        return true;
    }

    std::optional<std::string> get(std::string_view key) const {
        auto it = map_.find(std::string(key));
        if (it == map_.end()) {
            return std::nullopt;
        }
        const uint64_t now = now_sec();
        if (is_expired(it->second, now)) {
            return std::nullopt;
        }
        return it->second.value_;
    }

    std::vector<std::pair<std::string, std::string>> getManySorted(std::string_view key, uint32_t count) const {
        std::vector<std::pair<std::string, std::string>> res;
        res.reserve(count);
        const uint64_t now = now_sec();
        auto it = sorted_.upper_bound(std::string(key));
        for (; it != sorted_.end() && res.size() < count; ++it) {
            auto mit = map_.find(it->first);
            if (mit == map_.end()) {
                continue;
            }
            if (is_expired(mit->second, now)) {
                continue;
            }
            res.emplace_back(mit->first, mit->second.value_);
        }
        return res;
    }

    std::optional<std::pair<std::string, std::string>> removeOneExpiredEntry() {
        const uint64_t now = now_sec();
        while (!min_heap_.empty()) {
            HeapItem top = min_heap_.top();
            if (top.expired_at > now) {
                break;
            }
            min_heap_.pop();
            auto it = map_.find(top.key);
            if (it == map_.end()) {
                continue;
            }
            if (it->second.generation != top.generation) {
                continue;
            }
            if (it->second.expired_at != top.expired_at) {
                continue;
            }
            auto key = it->first;
            auto value = it->second.value_;
            sorted_.erase(it->first);
            map_.erase(it);
            return std::make_optional(std::make_pair(std::move(key), std::move(value)));
        }
        return std::nullopt;
    }

private:
    uint64_t now_sec() const {
        using namespace std::chrono;
        auto tp = clock_.now();
        return static_cast<uint64_t>(duration_cast<seconds>(tp.time_since_epoch()).count());
    }

    static constexpr uint64_t k_infinite = std::numeric_limits<uint64_t>::max();

    struct Entry {
        std::string value_;
        uint64_t expired_at = k_infinite;
        uint64_t generation = 0;
    };

    struct HeapItem {
        uint64_t expired_at;
        uint64_t generation;
        std::string key;
    };

    struct HeapComp {
        bool operator()(const HeapItem& x, const HeapItem& y) const {
            return x.expired_at > y.expired_at;
        }
    };

    static bool is_expired(const Entry& e, uint64_t now) {
        return e.expired_at != k_infinite && e.expired_at <= now;
    }

    void push_to_heap(const std::string& key, const Entry& e) {
        min_heap_.push({e.expired_at, e.generation, key});
    }

private:
    Clock clock_;
    std::unordered_map<std::string, Entry> map_;
    std::map<std::string, char> sorted_;
    std::priority_queue<HeapItem, std::vector<HeapItem>, HeapComp> min_heap_;
};