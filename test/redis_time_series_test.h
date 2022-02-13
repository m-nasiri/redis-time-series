#include "redis_time_series.h"
#include "gtest/gtest.h"
#include <sw/redis++/redis++.h>

namespace {

using namespace redis_time_series;

class TestCreate : public testing::Test {
  protected:
    void SetUp() override {
        inMemory_ = std::make_unique<sw::redis::Redis>("tcp://localhost:6379");
        inMemory_->del(key);
    }

    void TearDown() override { inMemory_->del(key); }

  public:
    std::unique_ptr<sw::redis::Redis> inMemory_;

  const std::string key = "CREATE_TESTS";
};

TEST_F(TestCreate, TestCreateOK) {
    ASSERT_TRUE(client::timeSeriesCreate(inMemory_.get(), key));
    auto info = client::timeSeriesInfo(inMemory_.get(), key);
}

} // namespace