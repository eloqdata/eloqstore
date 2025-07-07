#include "db_stress_common.h"

#include <cmath>
#include <cstdint>
#include <iterator>
#include <numeric>
#include <random>
#include <ranges>
#include <string>

#include "db_stress_shared_state.h"
#include "db_stress_test_base.h"

std::vector<double> sum_probs(100001);
constexpr int64_t zipf_sum_size = 100000;

namespace StressTest
{

void InitializeHotKeyGenerator(double alpha)
{
    double c = 0;
    for (int64_t i = 1; i <= zipf_sum_size; i++)
    {
        c = c +  (1.0 / std::pow(static_cast<double>(i), alpha));
    }
    c = 1.0 / c;

    sum_probs[0] = 0;
    for (int64_t i = 1; i <= zipf_sum_size; i++)
    {
        sum_probs[i] =
            sum_probs[i - 1] + c / std::pow(static_cast<double>(i), alpha);
    }
}

uint64_t GetOneHotKeyID(double rand_seed, int64_t max_key)
{
    int64_t low = 1, mid, high = zipf_sum_size, zipf = 0;
    while (low <= high)
    {
        mid = (low + high) / 2;
        if (sum_probs[mid] >= rand_seed && sum_probs[mid - 1] < rand_seed)
        {
            zipf = mid;
            break;
        }
        else if (sum_probs[mid] >= rand_seed)
        {
            high = mid - 1;
        }
        else
        {
            low = mid + 1;
        }
    }
    int64_t tmp_zipf_seed = zipf * max_key / zipf_sum_size;
    Random64 rand_local(tmp_zipf_seed);
    return rand_local.Next() % max_key;
}

int64_t GenerateOneKey(StressTest::Partition *partition, uint64_t iteration)
{
    const double completed_ratio =
        static_cast<double>(iteration) / FLAGS_ops_per_partition;
    const int64_t base_key = static_cast<int64_t>(
        completed_ratio * (FLAGS_max_key - FLAGS_active_width));
    int64_t rand_seed = base_key + partition->rand_.Next() % FLAGS_active_width;
    int64_t cur_key = rand_seed;
    if (FLAGS_hot_key_alpha != 0)
    {
        // If set the Zipfian distribution Alpha to non 0, use Zipfian
        double float_rand =
            (static_cast<double>(partition->rand_.Next() % FLAGS_max_key)) /
            FLAGS_max_key;
        cur_key = GetOneHotKeyID(float_rand, FLAGS_max_key);
    }
    return cur_key;
}

std::vector<int64_t> GenerateNKeys(StressTest::Partition *partition,
                                   uint64_t iteration)
{
    uint32_t n;
    if (FLAGS_keys_per_batch > 0)
        n = FLAGS_keys_per_batch;
    else
        n = partition->rand_.Uniform(400) + 100;
    assert(n > 0);
    std::vector<int64_t> cur_keys;
    std::unordered_set<int64_t> keys;

    if (FLAGS_hot_key_alpha == 0)
    {
        const double completed_ratio =
            static_cast<double>(iteration) / FLAGS_ops_per_partition;
        const int64_t base_key = static_cast<int64_t>(
            completed_ratio * (FLAGS_max_key - FLAGS_active_width));

        std::vector<int64_t> source(FLAGS_active_width);
        std::iota(source.begin(), source.end(), base_key);
        std::sample(source.begin(),
                    source.end(),
                    std::back_inserter(cur_keys),
                    n,
                    std::mt19937{std::random_device{}()});
    }

    else
    {
        size_t i = 0;
        while (keys.size() < n)
        {
            double float_rand =
                (static_cast<double>(partition->rand_.Next() % FLAGS_max_key)) /
                FLAGS_max_key;
            cur_keys.emplace_back(GetOneHotKeyID(float_rand, FLAGS_max_key));
            if (!keys.count(cur_keys[i]))
            {
                keys.insert(cur_keys[i]);
                ++i;
            }
        }
    }
    CHECK(cur_keys.size() == n);
    return cur_keys;
}

uint8_t JudgeKeyLevel(int64_t key)
{
    int offset = key % key_window;
    if (offset < 64)
    {
        return 0;
    }
    else if (offset < 96)
    {
        return 1;
    }
    else if (offset < 112)
    {
        return 2;
    }
    else
    {
        return 3;
    }
}

int64_t KeyStringToInt(std::string k)
{
    std::string k_base = k.substr(0, 8);
    int64_t key = std::stoi(k_base);
    CHECK(key < FLAGS_max_key);
    return key;
}

}  // namespace StressTest