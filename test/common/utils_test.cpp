#include "src/common/utils.h"

#include <gtest/gtest.h>

class UtilsTest : public ::testing::Test {};

TEST_F(UtilsTest, CheckMD5Test) {
    std::string data_1 = "123456789";
    auto md5_1 = dfs::common::calc_md5(data_1);
    std::cout << md5_1 << std::endl;
    std::cout << md5_1.size() << std::endl;

    std::string data_2 = "abcdefghij";
    auto md5_2 = dfs::common::calc_md5(data_2);
    std::cout << md5_2 << std::endl;
    std::cout << md5_2.size() << std::endl;

    EXPECT_TRUE(md5_1 != md5_2);
}

TEST_F(UtilsTest, CheckComputeHashTest) {
    std::string data_1 = "123456789";
    auto check_sum_1 = dfs::common::ComputeHash(data_1);
    std::cout << check_sum_1 << std::endl;
    std::cout << check_sum_1.size() << std::endl;

    std::string data_2 = "abcdefghij";
    auto check_sum_2 = dfs::common::ComputeHash(data_2);
    std::cout << check_sum_2 << std::endl;
    std::cout << check_sum_2.size() << std::endl;

    EXPECT_TRUE(check_sum_1 != check_sum_2);
}

TEST_F(UtilsTest, MemmoveTest) {
    void* buffer = malloc(100);
    std::string data = "0123456789";
    memmove((char*)buffer, data.c_str(), 5);
    memmove((char*)buffer + 4, data.c_str(), data.size());
    std::string output((char*)buffer);
    std::cout << output << std::endl;
    std::cout << output.size() << std::endl;
}