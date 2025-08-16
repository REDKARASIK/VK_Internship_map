#include "lib/kvstorage.h"
#include <gtest/gtest.h>
#include <chrono>

struct ManualClock {
    using clock      = std::chrono::steady_clock;
    using time_point = std::chrono::time_point<clock>;

    struct State { time_point now = time_point{}; };
    std::shared_ptr<State> s = std::make_shared<State>();

    time_point now() const { return s->now; }
    void advance(std::chrono::seconds dt) { s->now += dt; }
};

TEST(KV, BasicGetSet) {
    ManualClock clk;
    std::tuple<std::string,std::string,uint32_t> inits[] = {
        {"a","1",0}, {"b","2",0}
    };
    KVStorage<ManualClock> kv(std::span{inits}, clk);

    EXPECT_EQ(kv.get("a").value(), "1");
    EXPECT_EQ(kv.get("b").value(), "2");
    EXPECT_FALSE(kv.get("c").has_value());

    kv.set("a","11",0);
    EXPECT_EQ(kv.get("a").value(), "11");
}

TEST(KV, TTLExpire) {
    ManualClock clk;
    std::tuple<std::string,std::string,uint32_t> inits[] = {
        {"x","v",2},
    };
    KVStorage<ManualClock> kv(std::span{inits}, clk);

    EXPECT_TRUE(kv.get("x").has_value());
    clk.advance(std::chrono::seconds(1));
    EXPECT_TRUE(kv.get("x").has_value());
    clk.advance(std::chrono::seconds(2));
    EXPECT_FALSE(kv.get("x").has_value());
}

TEST(KV, SortedRange) {
    ManualClock clk;
    std::tuple<std::string,std::string,uint32_t> inits[] = {
        {"a","va",0}, {"b","vb",0}, {"d","vd",0}, {"e","ve",0}
    };
    KVStorage<ManualClock> kv(std::span{inits}, clk);

    auto v = kv.getManySorted("c", 2);
    ASSERT_EQ(v.size(), 2u);
    EXPECT_EQ(v[0].first, "d");
    EXPECT_EQ(v[0].second, "vd");
    EXPECT_EQ(v[1].first, "e");
    EXPECT_EQ(v[1].second, "ve");
}

TEST(KV, RemoveAndExpiredPop) {
    ManualClock clk;
    std::tuple<std::string,std::string,uint32_t> inits[] = {
        {"k1","v1",1}, {"k2","v2",1}, {"k3","v3",0}
    };
    KVStorage<ManualClock> kv(std::span{inits}, clk);

    EXPECT_TRUE(kv.remove("k3"));
    EXPECT_FALSE(kv.get("k3").has_value());

    clk.advance(std::chrono::seconds(2));
    auto p1 = kv.removeOneExpiredEntry();
    ASSERT_TRUE(p1.has_value());
    auto p2 = kv.removeOneExpiredEntry();
    ASSERT_TRUE(p2.has_value());
    auto p3 = kv.removeOneExpiredEntry();
    EXPECT_FALSE(p3.has_value());
}
