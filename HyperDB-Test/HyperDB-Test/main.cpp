#define CLUSTER_PERF_TEST
#ifdef CLUSTER_PERF_TEST
#include "include/HyperDB.h"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>

// the "idc - dev" timer
struct Timer {
    std::chrono::high_resolution_clock::time_point start;
    Timer() : start(std::chrono::high_resolution_clock::now()) {}
    double elapsed_seconds() {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start).count();
    }
};

// optimized-ish noise generator
// using mt19937 because rand() is a legacy joke from 1972
std::string fast_noise(size_t len, std::mt19937& rng) {
    static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) s += charset[dist(rng)];
    return s;
}

void wait_for_queue(HyperDBCluster& cluster) {
    while (!cluster.IsQueueEmpty()) {
        std::this_thread::yield();
    }
}

int main() {
    std::mt19937 rng(static_cast<unsigned int>(std::time(nullptr)));
    HyperDBCluster cluster;

    const std::string PWD = "death";
    const std::string FOLDER = "death_benchmark";
    const size_t SHARD_LIMIT = 512ULL * 1024 * 1024; // 512MB shards
    const int TOTAL_ROWS = 10000000;

    std::cout << "--- STARTING 10,000,000 ROW APOCALYPSE ---" << std::endl;
    std::cout << "[6:30PM] target: 3.8GB of raw encrypted entropy." << std::endl;
    std::cout << "[6:30PM] shard limit: 512MB. pray for the manifest." << std::endl;

    try {
        Timer total_timer;

        cluster.Open(FOLDER, "death", PWD, SHARD_LIMIT);
        cluster.QueueCreateTable("death", {
            {"noise1", HyperDB::ColumnType::ColumnType_String},
            {"noise2", HyperDB::ColumnType::ColumnType_String},
            {"noise3", HyperDB::ColumnType::ColumnType_String}
            });
        wait_for_queue(cluster);

        Timer loop_timer;
        for (int i = 1; i <= TOTAL_ROWS; ++i) {
            cluster.QueueWrite("death", {
                {"noise1", fast_noise(128, rng)},
                {"noise2", fast_noise(128, rng)},
                {"noise3", fast_noise(128, rng)}
                });

            // check memory sanity every 1M rows
            if (i % 1000000 == 0) {
                double pct = (static_cast<double>(i) / TOTAL_ROWS) * 100.0;
                std::cout << "  > progress: " << (int)pct << "% (" << i << " rows queued)..." << std::endl;

                // wait for background thread to catch up so we don't hit 32GB ram limit
                // if we don't do this, the std::queue will swallow your entire OS.
                // idc - dev.
                wait_for_queue(cluster);
            }
        }

        double loop_time = loop_timer.elapsed_seconds();
        std::cout << "[6:XXPM] loop finished in " << loop_time << "s. flushing remaining data..." << std::endl;

        Timer flush_timer;
        cluster.ForceFlush(58253); // the magic iteration count
        double flush_time = flush_timer.elapsed_seconds();

        double total_time = total_timer.elapsed_seconds();

        std::cout << "\n------------------------------------------" << std::endl;
        std::cout << "BENCHMARK COMPLETE" << std::endl;
        std::cout << "Total Time:    " << total_time << " seconds" << std::endl;
        std::cout << "Write Throughput: " << (TOTAL_ROWS / loop_time) << " rows/sec" << std::endl;
        std::cout << "Final Flush:   " << flush_time << " seconds" << std::endl;
        std::cout << "Shard Count:   " << cluster.GetShardCount() << std::endl;
        std::cout << "Total Rows:    " << cluster.GetRowCount("death") << std::endl;
        std::cout << "------------------------------------------" << std::endl;
        std::cout << "idc. go buy a new SSD." << std::endl;

    }
    catch (const std::exception& e) {
        std::cerr << "SYSTEM FAILURE: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#else
#include "include/HyperDB.h"
#include <iostream>
#include <iomanip>
#include <chrono>

#define ASSERT_TEST(condition, msg) \
    if (!(condition)) { \
        std::cerr << "[FAIL] " << msg << " (Line " << __LINE__ << ")" << std::endl; \
        std::exit(1); \
    } else { \
        std::cout << "[OK]   " << msg << std::endl; \
    }

// a simple timer because std::chrono is too verbose for my tired brain
struct Timer {
    std::chrono::high_resolution_clock::time_point start;
    Timer() : start(std::chrono::high_resolution_clock::now()) {}
    double elapsed_ms() {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }
};

// helper to block the main thread because async tests are a nightmare
template<typename T>
void wait_for_queue(T& db) {
    while (!db.IsQueueEmpty()) {
        std::this_thread::yield(); // tells the OS to let other threads breathe lol
    }
}

int main() {
    std::srand(static_cast<unsigned int>(std::time(nullptr)));
    std::cout << "--- INITIATING TIMED SPIRITUAL DESPAIR TEST SUITE ---" << std::endl;

    std::cout << "\n[1] TESTING HYPERDB MANAGER (SOLO)" << std::endl;
    {
        HyperDBManager db;

        {
            Timer t;
            db.OpenDB("test_solo.db", "sandwich");
            std::cout << "      > OpenDB took: " << t.elapsed_ms() << " ms" << std::endl;
        }

        db.QueueCreateTable("test_table", {
            {"id", HyperDB::ColumnType::ColumnType_Int32},
            {"username", HyperDB::ColumnType::ColumnType_String}
            });
        db.QueueClearTable("test_table");
        wait_for_queue(db);
        ASSERT_TEST(db.GetRowCount("test_table") == 0, "ClearTable reset row count to 0");

        {
            Timer t;
            db.QueueWrite("test_table", { {"id", int32_t(100)}, {"username", std::string("snayck")} });
            db.QueueWrite("test_table", { {"id", int32_t(200)}, {"username", std::string("batman")} });
            wait_for_queue(db);
            std::cout << "      > QueueWrite (2 rows) + Queue Drain took: " << t.elapsed_ms() << " ms" << std::endl;
        }
        ASSERT_TEST(db.GetRowCount("test_table") == 2, "Write successfully added 2 rows");

        {
            bool read_success = false;
            Timer t;
            db.QueueRead("test_table", 1, [&](ReadResult res) {
                if (res.size() == 2 && HyperDBUtil::HyperValueToString(res[1].value) == "batman") {
                    read_success = true;
                }
                });
            wait_for_queue(db);
            std::cout << "      > QueueRead + Callback took: " << t.elapsed_ms() << " ms" << std::endl;
            ASSERT_TEST(read_success, "Read successfully retrieved batman at row 1");
        }

        {
            bool find_success = false;
            Timer t;
            db.QueueFind("test_table", "username", std::string("snayck"), [&](std::vector<ReadResult> results) {
                if (results.size() == 1 && HyperDBUtil::HyperValueToString(results[0][0].value) == "100") {
                    find_success = true;
                }
                });
            wait_for_queue(db);
            std::cout << "      > QueueFind + Search logic took: " << t.elapsed_ms() << " ms" << std::endl;
            ASSERT_TEST(find_success, "Find successfully located snayck and matched id 100");
        }

        {
            Timer t;
            db.QueueDelete("test_table", "username", std::string("snayck"));
            wait_for_queue(db);
            std::cout << "      > QueueDelete + Vector Erasure took: " << t.elapsed_ms() << " ms" << std::endl;
        }
        ASSERT_TEST(db.GetRowCount("test_table") == 1, "Delete successfully removed 1 row");

        // cause exception on purpose, part of the unit test.
        db.QueueClearColumn("test_table", "username");
        wait_for_queue(db);
        bool exception_caught = false;
        db.QueueRead("test_table", 0, [&](ReadResult res) {
            if (res.empty()) exception_caught = true;
            });
        wait_for_queue(db);
        ASSERT_TEST(exception_caught, "ClearColumn caused expected out-of-bounds protection to fire");

        {
            Timer t;
            db.ForceFlush(58253); // 58253 iterations of sha256
            std::cout << "      > ForceFlush (58253 iterations) took: " << t.elapsed_ms() << " ms" << std::endl;
        }
    }

    std::cout << "\n[2] TESTING HYPERDB CLUSTER (DISTRIBUTED MOJIBAKE)" << std::endl;
    {
        HyperDBCluster cluster;
        {
            Timer t;
            cluster.Open("test_cluster", "chaos", "death", 500);
            std::cout << "      > Cluster Open + Manifest Load took: " << t.elapsed_ms() << " ms" << std::endl;
        }

        cluster.QueueCreateTable("cluster_logs", {
            {"log_id", HyperDB::ColumnType::ColumnType_Int64},
            {"message", HyperDB::ColumnType::ColumnType_String}
            });
        cluster.QueueClearTable("cluster_logs");
        wait_for_queue(cluster);

        {
            Timer t;
            for (int i = 0; i < 10; ++i) {
                cluster.QueueWrite("cluster_logs", {
                    {"log_id", int64_t(i)},
                    {"message", std::string("this is a long string to trigger a shard spillover event right now ") + std::to_string(i)}
                    });
                wait_for_queue(cluster);
            }
            std::cout << "      > Sharding 10 rows into multiple files took: " << t.elapsed_ms() << " ms" << std::endl;
        }

        int shard_count = cluster.GetShardCount();
        ASSERT_TEST(shard_count > 1, std::string("Cluster successfully spilled into " + std::to_string(shard_count) + " shards"));

        {
            bool read_success = false;
            Timer t;
            cluster.QueueRead("cluster_logs", 8, [&](ReadResult res) {
                if (res.size() == 2 && HyperDBUtil::HyperValueToString(res[0].value) == "8") {
                    read_success = true;
                }
                });
            wait_for_queue(cluster);
            std::cout << "      > Cross-shard Read (Id resolution) took: " << t.elapsed_ms() << " ms" << std::endl;
            ASSERT_TEST(read_success, "Cross-shard Read successfully resolved global row 8 to its local shard");
        }

        {
            bool find_success = false;
            Timer t;
            cluster.QueueFind("cluster_logs", "log_id", int64_t(3), [&](std::vector<ReadResult> results) {
                if (results.size() == 1) find_success = true;
                });
            wait_for_queue(cluster);
            std::cout << "      > Cross-shard Find (Multi-shard aggregation) took: " << t.elapsed_ms() << " ms" << std::endl;
            ASSERT_TEST(find_success, "Cross-shard Find successfully aggregated results from multiple shards");
        }

        {
            Timer t;
            cluster.ForceFlush(58253);
            std::cout << "      > Cluster ForceFlush (All Shards - 58253i) took: " << t.elapsed_ms() << " ms" << std::endl;
        }
    }

    std::cout << "\n---------------------------------------------------" << std::endl;
    std::cout << "ALL TESTS PASSED. TIMINGS LOGGED." << std::endl;
    std::cout << "turn your pc off before it starts mining litecoin." << std::endl;
    std::cout << "---------------------------------------------------" << std::endl;

    return 0;
}
#endif