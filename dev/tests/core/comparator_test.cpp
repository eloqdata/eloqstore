#include <catch2/catch_test_macros.hpp>
#include <algorithm>
#include <random>
#include <set>

#include "comparator.h"
#include "fixtures/test_helpers.h"
#include "fixtures/data_generator.h"

using namespace eloqstore;
using namespace eloqstore::test;

TEST_CASE("Comparator_DefaultComparator_BasicComparisons", "[comparator][unit]") {
    const Comparator* comp = Comparator::DefaultComparator();

    SECTION("Compare equal strings") {
        REQUIRE(comp->Compare("abc", "abc") == 0);
        REQUIRE(comp->Compare("", "") == 0);
        REQUIRE(comp->Compare("12345", "12345") == 0);
    }

    SECTION("Compare different strings") {
        REQUIRE(comp->Compare("a", "b") < 0);
        REQUIRE(comp->Compare("b", "a") > 0);
        REQUIRE(comp->Compare("abc", "abd") < 0);
        REQUIRE(comp->Compare("abd", "abc") > 0);
    }

    SECTION("Compare different length strings") {
        REQUIRE(comp->Compare("a", "aa") < 0);
        REQUIRE(comp->Compare("aa", "a") > 0);
        REQUIRE(comp->Compare("", "a") < 0);
        REQUIRE(comp->Compare("a", "") > 0);
    }

    SECTION("Lexicographic ordering") {
        std::vector<std::string> strings = {"zebra", "apple", "banana", "cherry"};
        std::sort(strings.begin(), strings.end(),
                 [comp](const std::string& a, const std::string& b) {
                     return comp->Compare(a, b) < 0;
                 });

        REQUIRE(strings == std::vector<std::string>{"apple", "banana", "cherry", "zebra"});
    }
}

TEST_CASE("Comparator_DefaultComparator_EdgeCases", "[comparator][unit][edge-case]") {
    const Comparator* comp = Comparator::DefaultComparator();

    SECTION("Empty strings") {
        REQUIRE(comp->Compare("", "") == 0);
        REQUIRE(comp->Compare("", "a") < 0);
        REQUIRE(comp->Compare("a", "") > 0);
    }

    SECTION("Null bytes in strings") {
        std::string s1("abc", 3);
        s1.append(1, '\0');
        s1.append("def", 3);
        std::string s2("abc", 3);
        s2.append(1, '\0');
        s2.append("xyz", 3);
        std::string s3 = "abc";

        // Strings with null bytes
        REQUIRE(comp->Compare(s1, s1) == 0);
        REQUIRE(comp->Compare(s1, s2) < 0);  // def < xyz
        REQUIRE(comp->Compare(s2, s1) > 0);

        // Null byte should be less than any character
        REQUIRE(comp->Compare(std::string("a\x00", 2), "ab") < 0);
        REQUIRE(comp->Compare("ab", std::string("a\x00", 2)) > 0);
    }

    SECTION("Special characters") {
        // Test with various special characters
        REQUIRE(comp->Compare("\x01", "\x02") < 0);
        REQUIRE(comp->Compare("\xFF", "\x00") > 0);
        REQUIRE(comp->Compare("\t", " ") < 0);  // Tab vs space
        REQUIRE(comp->Compare("\n", "\r") < 0);  // Newline vs carriage return
    }

    SECTION("Binary data comparison") {
        std::string binary1;
        std::string binary2;

        for (int i = 0; i < 256; ++i) {
            binary1 += static_cast<char>(i);
            binary2 += static_cast<char>(255 - i);
        }

        REQUIRE(comp->Compare(binary1, binary1) == 0);
        REQUIRE(comp->Compare(binary1, binary2) < 0);
        REQUIRE(comp->Compare(binary2, binary1) > 0);
    }

    SECTION("Maximum length strings") {
        std::string long1(10000, 'a');
        std::string long2(10000, 'a');
        std::string long3(10000, 'b');

        REQUIRE(comp->Compare(long1, long2) == 0);
        REQUIRE(comp->Compare(long1, long3) < 0);
        REQUIRE(comp->Compare(long3, long1) > 0);

        // Differ only at the end
        long2[9999] = 'b';
        REQUIRE(comp->Compare(long1, long2) < 0);
    }

    SECTION("UTF-8 strings") {
        // UTF-8 strings should be compared byte-by-byte
        std::string utf8_1 = "Hello 世界";
        std::string utf8_2 = "Hello 世界";
        std::string utf8_3 = "Hello 中国";

        REQUIRE(comp->Compare(utf8_1, utf8_2) == 0);
        REQUIRE(comp->Compare(utf8_1, utf8_3) != 0);

        // UTF-8 ordering should be consistent
        std::vector<std::string> utf8_strings = {
            "café", "caff", "😀", "😁", "中文", "日本語"
        };

        std::sort(utf8_strings.begin(), utf8_strings.end(),
                 [comp](const std::string& a, const std::string& b) {
                     return comp->Compare(a, b) < 0;
                 });

        // Verify sorted order is stable
        for (size_t i = 1; i < utf8_strings.size(); ++i) {
            REQUIRE(comp->Compare(utf8_strings[i-1], utf8_strings[i]) <= 0);
        }
    }
}

TEST_CASE("Comparator_DefaultComparator_Properties", "[comparator][unit]") {
    const Comparator* comp = Comparator::DefaultComparator();
    DataGenerator gen(42);

    SECTION("Transitivity") {
        // If a < b and b < c, then a < c
        std::vector<std::string> strings = gen.GenerateSortedKeys(100, 10);

        for (size_t i = 0; i < strings.size() - 2; ++i) {
            const auto& a = strings[i];
            const auto& b = strings[i + 1];
            const auto& c = strings[i + 2];

            if (comp->Compare(a, b) < 0 && comp->Compare(b, c) < 0) {
                REQUIRE(comp->Compare(a, c) < 0);
            }
        }
    }

    SECTION("Antisymmetry") {
        // If a <= b and b <= a, then a == b
        auto strings = gen.GenerateRandomKeys(50, 10);

        for (const auto& a : strings) {
            for (const auto& b : strings) {
                if (comp->Compare(a, b) <= 0 && comp->Compare(b, a) <= 0) {
                    REQUIRE(a == b);
                }
            }
        }
    }

    SECTION("Reflexivity") {
        // a == a for all a
        auto strings = gen.GenerateRandomKeys(100, 20);

        for (const auto& s : strings) {
            REQUIRE(comp->Compare(s, s) == 0);
        }
    }

    SECTION("Consistency") {
        // Multiple comparisons should yield same result
        auto strings = gen.GenerateRandomKeys(50, 15);

        for (const auto& a : strings) {
            for (const auto& b : strings) {
                int result1 = comp->Compare(a, b);
                int result2 = comp->Compare(a, b);
                int result3 = comp->Compare(a, b);

                REQUIRE(result1 == result2);
                REQUIRE(result2 == result3);
            }
        }
    }
}

TEST_CASE("Comparator_DefaultComparator_Performance", "[comparator][benchmark]") {
    const Comparator* comp = Comparator::DefaultComparator();
    DataGenerator gen(42);

    SECTION("Short string comparisons") {
        auto strings = gen.GenerateRandomKeys(1000, 10);
        Timer timer;

        const int iterations = 100000;
        timer.Start();

        for (int i = 0; i < iterations; ++i) {
            comp->Compare(strings[i % strings.size()],
                         strings[(i + 1) % strings.size()]);
        }

        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        // LOG(INFO) << "Short string comparisons: " << ops_per_sec << " ops/sec";

        // Should handle at least 1M comparisons per second
        REQUIRE(ops_per_sec > 1000000);
    }

    SECTION("Long string comparisons") {
        // Generate long strings that are mostly identical
        std::string base(1000, 'x');
        std::vector<std::string> strings;

        for (int i = 0; i < 100; ++i) {
            std::string s = base;
            s[990 + (i % 10)] = 'a' + (i % 26);
            strings.push_back(s);
        }

        Timer timer;
        const int iterations = 100000;
        timer.Start();

        for (int i = 0; i < iterations; ++i) {
            comp->Compare(strings[i % strings.size()],
                         strings[(i + 1) % strings.size()]);
        }

        timer.Stop();

        double ops_per_sec = iterations / timer.ElapsedSeconds();
        // LOG(INFO) << "Long string comparisons: " << ops_per_sec << " ops/sec";

        // Should still be reasonably fast
        REQUIRE(ops_per_sec > 100000);
    }
}

TEST_CASE("Comparator_DefaultComparator_Sorting", "[comparator][unit]") {
    const Comparator* comp = Comparator::DefaultComparator();
    DataGenerator gen(42);

    SECTION("Sort random keys") {
        auto keys = gen.GenerateRandomKeys(100, 20);

        std::sort(keys.begin(), keys.end(),
                 [comp](const std::string& a, const std::string& b) {
                     return comp->Compare(a, b) < 0;
                 });

        // Verify sorted order
        for (size_t i = 1; i < keys.size(); ++i) {
            REQUIRE(comp->Compare(keys[i-1], keys[i]) <= 0);
        }
    }

    SECTION("Sort with duplicates") {
        std::vector<std::string> keys;

        // Add duplicates
        for (int i = 0; i < 20; ++i) {
            std::string key = "key_" + std::to_string(i % 10);
            keys.push_back(key);
        }

        std::sort(keys.begin(), keys.end(),
                 [comp](const std::string& a, const std::string& b) {
                     return comp->Compare(a, b) < 0;
                 });

        // Verify sorted order with duplicates
        for (size_t i = 1; i < keys.size(); ++i) {
            REQUIRE(comp->Compare(keys[i-1], keys[i]) <= 0);
        }

        // Count duplicates
        std::map<std::string, int> counts;
        for (const auto& key : keys) {
            counts[key]++;
        }

        for (const auto& [key, count] : counts) {
            REQUIRE(count == 2);  // Each key should appear twice
        }
    }

    SECTION("Stable sort property") {
        // Create keys with associated values
        std::vector<std::pair<std::string, int>> key_values;

        for (int i = 0; i < 50; ++i) {
            key_values.push_back({"key_" + std::to_string(i % 10), i});
        }

        std::stable_sort(key_values.begin(), key_values.end(),
                        [comp](const auto& a, const auto& b) {
                            return comp->Compare(a.first, b.first) < 0;
                        });

        // Verify that equal keys maintain relative order
        for (size_t i = 1; i < key_values.size(); ++i) {
            if (key_values[i-1].first == key_values[i].first) {
                REQUIRE(key_values[i-1].second < key_values[i].second);
            }
        }
    }
}

TEST_CASE("Comparator_CustomComparator", "[comparator][unit]") {
    // Test that custom comparators can be created and used

    class ReverseComparator : public Comparator {
    public:
        int Compare(std::string_view a, std::string_view b) const override {
            // Reverse order
            return -Comparator::DefaultComparator()->Compare(a, b);
        }

        const char* Name() const override {
            return "ReverseComparator";
        }

        void FindShortestSeparator(std::string* start, std::string_view limit) const override {
            // Use default implementation
            Comparator::DefaultComparator()->FindShortestSeparator(start, limit);
        }

        std::string_view FindShortestSeparator(std::string_view left, std::string_view right) const override {
            // Use default implementation
            return Comparator::DefaultComparator()->FindShortestSeparator(left, right);
        }

        void FindShortSuccessor(std::string* key) const override {
            // Use default implementation
            Comparator::DefaultComparator()->FindShortSuccessor(key);
        }
    };

    ReverseComparator rev_comp;

    SECTION("Reverse ordering") {
        REQUIRE(rev_comp.Compare("a", "b") > 0);  // Reversed
        REQUIRE(rev_comp.Compare("b", "a") < 0);  // Reversed
        REQUIRE(rev_comp.Compare("abc", "abc") == 0);
    }

    SECTION("Sort with reverse comparator") {
        std::vector<std::string> strings = {"apple", "zebra", "banana", "cherry"};

        std::sort(strings.begin(), strings.end(),
                 [&rev_comp](const std::string& a, const std::string& b) {
                     return rev_comp.Compare(a, b) < 0;
                 });

        // Should be in reverse order
        REQUIRE(strings == std::vector<std::string>{"zebra", "cherry", "banana", "apple"});
    }
}

TEST_CASE("Comparator_StressTest", "[comparator][stress]") {
    const Comparator* comp = Comparator::DefaultComparator();
    DataGenerator gen(42);

    SECTION("Compare all permutations") {
        // Generate a small set and compare all permutations
        auto keys = gen.GenerateRandomKeys(20, 10);

        for (const auto& a : keys) {
            for (const auto& b : keys) {
                int result = comp->Compare(a, b);

                if (a == b) {
                    REQUIRE(result == 0);
                } else if (a < b) {
                    REQUIRE(result < 0);
                } else {
                    REQUIRE(result > 0);
                }

                // Inverse comparison
                int inverse = comp->Compare(b, a);
                if (result < 0) {
                    REQUIRE(inverse > 0);
                } else if (result > 0) {
                    REQUIRE(inverse < 0);
                } else {
                    REQUIRE(inverse == 0);
                }
            }
        }
    }

    SECTION("Random comparison patterns") {
        const int num_patterns = 10;
        const int keys_per_pattern = 100;

        for (int pattern = 0; pattern < num_patterns; ++pattern) {
            std::vector<std::string> keys;

            switch (pattern % 5) {
                case 0:  // Random keys
                    keys = gen.GenerateRandomKeys(keys_per_pattern, 20);
                    break;
                case 1:  // Sequential keys
                    for (int i = 0; i < keys_per_pattern; ++i) {
                        keys.push_back(gen.GenerateSequentialKey(i));
                    }
                    break;
                case 2:  // Reverse sequential
                    for (int i = keys_per_pattern - 1; i >= 0; --i) {
                        keys.push_back(gen.GenerateSequentialKey(i));
                    }
                    break;
                case 3:  // Clustered keys
                    for (const auto& key : gen.GenerateClusteredKeys(10, 10)) {
                        keys.push_back(key);
                    }
                    break;
                case 4:  // Binary keys
                    for (int i = 0; i < keys_per_pattern; ++i) {
                        keys.push_back(gen.GenerateBinaryString(20));
                    }
                    break;
            }

            // Use comparator with std::set to verify correctness
            std::set<std::string, std::function<bool(const std::string&, const std::string&)>>
                sorted_set([comp](const std::string& a, const std::string& b) {
                    return comp->Compare(a, b) < 0;
                });

            sorted_set.insert(keys.begin(), keys.end());

            // Verify set maintains order
            std::string prev;
            bool first = true;
            for (const auto& key : sorted_set) {
                if (!first) {
                    REQUIRE(comp->Compare(prev, key) < 0);
                }
                prev = key;
                first = false;
            }
        }
    }
}