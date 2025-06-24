#include "kill_point.h"

#include "external/random.h"

namespace eloqstore
{
KillPoint &KillPoint::GetInstance()
{
    static KillPoint kill_point;
    return kill_point;
}

void KillPoint::TestKillRandom(std::string kill_point,
                               const char *file,
                               uint32_t line,
                               const char *fn,
                               uint32_t odds_weight)
{
    if (kill_odds_ == 0)
    {
        return;
    }

    uint64_t odds = kill_odds_ * odds_weight;
    assert(odds > 0);
    auto *r = rocksdb::Random::GetTLSInstance();
    bool crash = r->OneIn(odds);
    if (crash)
    {
        fprintf(stdout, "Crashing at %s:%d, function %s\n", file, line, fn);
        fflush(stdout);
        kill(getpid(), SIGTERM);
    }
}
}  // namespace eloqstore