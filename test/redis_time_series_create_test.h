#include "redis_time_series.h"
#include "gtest/gtest.h"
#include <sw/redis++/redis++.h>

namespace {

using namespace redis_time_series;

class TestCreate : public testing::Test {
  public:
    TestCreate()
        : inMemory_{
              std::make_unique<sw::redis::Redis>("tcp://localhost:6379")} {}

    std::unique_ptr<sw::redis::Redis> inMemory_;
    const std::string key = "CREATE_TESTS";

  protected:
    void SetUp() override {}
    void TearDown() override { inMemory_->del(key); }
};

TEST_F(TestCreate, TestCreateOK) {
    ASSERT_TRUE(client::timeSeriesCreate(inMemory_.get(), key));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
}

TEST_F(TestCreate, TestCreateRetentionTime) {
    uint64_t retentionTime{5000};
    ASSERT_TRUE(client::timeSeriesCreate(inMemory_.get(), key, retentionTime));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
    ASSERT_EQ(retentionTime, info.retentionTime());
}

TEST_F(TestCreate, TestCreateLabels) {
    TimeSeriesLabel label{"key", "value"};
    std::vector<TimeSeriesLabel> labels{label};
    ASSERT_TRUE(
        client::timeSeriesCreate(inMemory_.get(), key, std::nullopt, labels));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
    ASSERT_EQ(labels, info.labels());
}

TEST_F(TestCreate, TestCreateEmptyLabel) {
    std::vector<TimeSeriesLabel> labels;
    ASSERT_TRUE(
        client::timeSeriesCreate(inMemory_.get(), key, std::nullopt, labels));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
    ASSERT_EQ(labels, info.labels());
}

TEST_F(TestCreate, TestCreateUncompressed) {
    ASSERT_TRUE(
        client::timeSeriesCreate(inMemory_.get(), key, std::nullopt, {}, true));
}

TEST_F(TestCreate, TestCreatehDuplicatePolicyFirst) {
    ASSERT_TRUE(client::timeSeriesCreate(
        inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
        command_operator::TsDuplicatePolicy::FIRST));
}

TEST_F(TestCreate, TestCreatehDuplicatePolicyLast) {
    ASSERT_TRUE(client::timeSeriesCreate(
        inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
        command_operator::TsDuplicatePolicy::LAST));
}

TEST_F(TestCreate, TestCreatehDuplicatePolicyMin) {
    ASSERT_TRUE(client::timeSeriesCreate(
        inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
        command_operator::TsDuplicatePolicy::MIN));
}

TEST_F(TestCreate, TestCreatehDuplicatePolicyMax) {
    ASSERT_TRUE(client::timeSeriesCreate(
        inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
        command_operator::TsDuplicatePolicy::MAX));
}

TEST_F(TestCreate, TestCreatehDuplicatePolicySum) {
    ASSERT_TRUE(client::timeSeriesCreate(
        inMemory_.get(), key, std::nullopt, {}, std::nullopt, std::nullopt,
        command_operator::TsDuplicatePolicy::SUM));
}

} // namespace