#include "src/client/client_cache_manager.h"

#include <gtest/gtest.h>

using namespace dfs::client;

class ClientCacheManagerTest : public ::testing::Test {
    protected:
        CacheManager cache_;
};

TEST_F(ClientCacheManagerTest, EmptySetTest) {
    std::cout << "ok" << std::endl;
    cache_.GetChunkHandle("/file", 0);
    // cache_.SetChunkHandle("/file", 0, "0");
}