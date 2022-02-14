#include "redis_time_series.h"
#include "gtest/gtest.h"
#include <sw/redis++/redis++.h>

namespace {

using namespace redis_time_series;

class TestAdd : public testing::Test {
  public:
    TestAdd()
        : inMemory_{
              std::make_unique<sw::redis::Redis>("tcp://localhost:6379")} {}

    std::unique_ptr<sw::redis::Redis> inMemory_;
    const std::string key = "ADD_TESTS";

  protected:
    void SetUp() override {}
    void TearDown() override { inMemory_->del(key); }
};

TEST_F(TestAdd, TestAddNotExistingTimeSeries) {
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    ASSERT_EQ(now, client::timeSeriesAdd(inMemory_.get(), key, now, 1.1));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
    ASSERT_EQ(now, info.firstTimeStamp());
    ASSERT_EQ(now, info.lastTimeStamp());
}

TEST_F(TestAdd, TestAddExistingTimeSeries) {
    client::timeSeriesCreate(inMemory_.get(), key);
    auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
                   .count();
    ASSERT_EQ(now, client::timeSeriesAdd(inMemory_.get(), key, now, 1.1));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
    ASSERT_EQ(now, info.firstTimeStamp());
    ASSERT_EQ(now, info.lastTimeStamp());
}

TEST_F(TestAdd, TestAddStar) {
    client::timeSeriesAdd(inMemory_.get(), key, TimeStamp("*"), 1.1);
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
    ASSERT_TRUE(TimeStamp{0} < info.firstTimeStamp());
    ASSERT_EQ(info.lastTimeStamp(), info.firstTimeStamp());
}

// TEST_F(TestAdd, TestAddEmptyLabel) {
//     std::vector<TimeSeriesLabel> labels;
//     ASSERT_TRUE(
//         client::timeSeriesCreate(inMemory_.get(), key, std::nullopt,
//         labels));
//     auto info = client::timeSeriesInfo(inMemory_.get(), key);
//     ASSERT_EQ(labels, info.labels());
// }

// TEST_F(TestAdd, TestAddUncompressed) {
//     ASSERT_TRUE(
//         client::timeSeriesCreate(inMemory_.get(), key, std::nullopt, {},
//         true));
// }

// TEST_F(TestAdd, TestAddhDuplicatePolicyFirst) {
//     ASSERT_TRUE(client::timeSeriesCreate(
//         inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
//         command_operator::TsDuplicatePolicy::FIRST));
// }

// TEST_F(TestAdd, TestAddhDuplicatePolicyLast) {
//     ASSERT_TRUE(client::timeSeriesCreate(
//         inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
//         command_operator::TsDuplicatePolicy::LAST));
// }

// TEST_F(TestAdd, TestAddhDuplicatePolicyMin) {
//     ASSERT_TRUE(client::timeSeriesCreate(
//         inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
//         command_operator::TsDuplicatePolicy::MIN));
// }

// TEST_F(TestAdd, TestAddhDuplicatePolicyMax) {
//     ASSERT_TRUE(client::timeSeriesCreate(
//         inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
//         command_operator::TsDuplicatePolicy::MAX));
// }

// TEST_F(TestAdd, TestAddhDuplicatePolicySum) {
//     ASSERT_TRUE(client::timeSeriesCreate(
//         inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
//         command_operator::TsDuplicatePolicy::SUM));
// }

} // namespace