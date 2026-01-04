#pragma once

#include <glog/logging.h>

#include <cassert>

#ifndef NDEBUG
#define TEST_KILL_POINT_WEIGHT(kill_point, odds_weight) \
    KillPoint::GetInstance().TestKillRandom(            \
        kill_point, __FILE__, __LINE__, __func__, odds_weight);
#else
#define TEST_KILL_POINT_WEIGHT(kill_point, odds_weight)
#endif

#define TEST_KILL_POINT(kill_point) TEST_KILL_POINT_WEIGHT(kill_point, 1)

namespace eloqstore
{
class KillPoint
{
public:
    static KillPoint &GetInstance();
    void TestKillRandom(std::string kill_point,
                        const char *file,
                        uint32_t line,
                        const char *fn,
                        uint32_t odds_weight = 1);

    uint32_t kill_odds_{0};
};
}  // namespace eloqstore
