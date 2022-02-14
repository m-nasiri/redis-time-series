#include "redis_time_series_add_test.h"
#include "redis_time_series_create_test.h"
#include "gtest/gtest.h"

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}