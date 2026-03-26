//#define CLUSTER_PERF_TEST
#define CLUSTER_READ_TEST

#ifdef CLUSTER_READ_TEST
#include "include/HyperDB.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <random>
#include <atomic>

struct Timer {
    std::chrono::high_resolution_clock::time_point start;
    Timer() : start(std::chrono::high_resolution_clock::now()) {}
    double elapsed_seconds() {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double>(end - start).count();
    }
};

void wait_for_queue(HyperDBCluster& cluster) {
    while (!cluster.IsQueueEmpty()) {
        std::this_thread::yield();
    }
}

int main() {
    HyperDBCluster cluster;
    const std::string PWD = "death";
    const std::string FOLDER = "death_benchmark";

    std::cout << "--- INITIATING READ APOCALYPSE ---" << std::endl;

    try {
        Timer load_timer;
        cluster.Open(FOLDER, "death", PWD);
        std::cout << "> Shards Loaded: " << cluster.GetShardCount() << " in " << load_timer.elapsed_seconds() << "s" << std::endl;

        int total_rows = cluster.GetRowCount("death");
        std::cout << "> Total Rows in Dataset: " << total_rows << std::endl;

        // 1. Stress Test Global ID Resolution (100,000 random reads)
        const int READ_COUNT = 100000;
        std::atomic<int> completed_reads{ 0 };
        std::mt19937 rng(1337);
        std::uniform_int_distribution<int> dist(0, total_rows - 1);

        std::cout << "> Stress testing 100,000 random global ID reads..." << std::endl;
        Timer read_timer;
        for (int i = 0; i < READ_COUNT; ++i) {
            cluster.QueueRead("death", dist(rng), [&](ReadResult res) {
                completed_reads++;
            });
            
            // throttle a bit so we don't just allocate 100k callbacks instantly and die
            if (i % 5000 == 0) wait_for_queue(cluster);
        }
        wait_for_queue(cluster);
        double read_time = read_timer.elapsed_seconds();

        std::cout << "  - 100k Reads took: " << read_time << "s (" << (READ_COUNT / read_time) << " reads/sec)" << std::endl;

        // 2. Multi-Shard Aggregate Find (Aggregating across 8 physical files)
        // We look for a string that likely won't exist just to force a full scan of all shards
        std::cout << "> Multi-shard scan for non-existent value (full aggregation)..." << std::endl;
        Timer find_timer;
        cluster.QueueFind("death", "noise1", std::string("missing_no_9999"), [](std::vector<ReadResult> results) {
            std::cout << "  - Scan completed. Found " << results.size() << " results." << std::endl;
        });
        wait_for_queue(cluster);
        std::cout << "  - Full shard-aggregated scan took: " << find_timer.elapsed_seconds() << "s" << std::endl;

    } catch (const std::exception& e) {
        std::cerr << "READ FAILURE: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif

#ifdef CLUSTER_PERF_TEST
#include "include/HyperDB.h"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <iomanip>
#include <sstream>
#include <ctime>

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

std::string get_time_stamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm bt;
    localtime_s(&bt, &in_time_t);
    std::stringstream ss;
    ss << std::put_time(&bt, "[%I:%M%p]");
    return ss.str();
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
    std::cout << get_time_stamp() << " target: 3.8GB of raw encrypted entropy." << std::endl;
    std::cout << get_time_stamp() << " shard limit: 512MB. pray for the manifest." << std::endl;

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
        Timer lap_timer;
        const int BATCH_SIZE = 10000;
        for (int i = 0; i < TOTAL_ROWS; i += BATCH_SIZE) {
            std::vector<std::vector<RowData>> batch;
            batch.reserve(BATCH_SIZE);
            for (int j = 0; j < BATCH_SIZE; ++j) {
                batch.push_back({
                    {"noise1", fast_noise(128, rng)},
                    {"noise2", fast_noise(128, rng)},
                    {"noise3", fast_noise(128, rng)}
                    });
            }
            cluster.QueueWriteBulk("death", std::move(batch));

            // check memory sanity every 1M rows
            int current_rows = i + BATCH_SIZE;
            if (current_rows % 1000000 == 0) {
                double pct = (static_cast<double>(current_rows) / TOTAL_ROWS) * 100.0;
                double lap_time = lap_timer.elapsed_seconds();
                std::cout << "  > progress: " << (int)pct << "% (" << current_rows << " rows queued). Last 1M took: " << lap_time << "s" << std::endl;
                lap_timer = Timer(); // restart lap timer for next mill

                // wait for background thread to catch up so we don't hit 32GB ram limit
                wait_for_queue(cluster);
            }
        }

        double loop_time = loop_timer.elapsed_seconds();
        std::cout << get_time_stamp() << " loop finished in " << loop_time << "s. flushing remaining data..." << std::endl;

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
#ifndef CLUSTER_READ_TEST
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
#endif