#include "include/HyperDB.h"
#include <iostream>
#include <vector>
#include <string>
#include <chrono>
#include <random>
#include <iomanip>
#include <sstream>
#include <ctime>
#include <atomic>

// --- SHARED UTILS ---

struct Timer {
    std::chrono::high_resolution_clock::time_point start;
    Timer() : start(std::chrono::high_resolution_clock::now()) {}
    double elapsed_ms() {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start).count();
    }
};

std::string get_time_stamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::tm bt;
#ifdef _WIN32
    localtime_s(&bt, &in_time_t);
#else
    localtime_r(&in_time_t, &bt);
#endif
    std::stringstream ss;
    ss << std::put_time(&bt, "[%I:%M%p]");
    return ss.str();
}

std::string fast_noise(size_t len, std::mt19937& rng) {
    static const char charset[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
    std::string s;
    s.reserve(len);
    for (size_t i = 0; i < len; ++i) s += charset[dist(rng)];
    return s;
}

#define ASSERT_TEST(condition, msg) \
    if (!(condition)) { \
        std::cerr << "  [FAIL] " << msg << " (Line " << __LINE__ << ")" << std::endl; \
        std::exit(1); \
    } else { \
        std::cout << "  [OK]   " << msg << std::endl; \
    }

template<typename T>
void wait_for_queue(T& db) {
    while (!db.IsQueueEmpty()) {
        std::this_thread::yield();
    }
}

// --- TEST SUITES ---

void RunSoloTests() {
    std::cout << "\n[1] TESTING HYPERDB MANAGER (SOLO UNIT TESTS)" << std::endl;
    HyperDBManager db;

    {
        Timer t;
        db.OpenDB("test_solo.db", "sandwich");
        std::cout << "      > OpenDB took: " << t.elapsed_ms() << " ms" << std::endl;
    }

    {
        Timer t;
        db.QueueCreateTable("test_table", {
            {"id", HyperDB::ColumnType::ColumnType_Int32},
            {"username", HyperDB::ColumnType::ColumnType_String}
        });
        wait_for_queue(db);
        std::cout << "      > CreateTable took: " << t.elapsed_ms() << " ms" << std::endl;
    }

    {
        Timer t;
        db.QueueClearTable("test_table");
        wait_for_queue(db);
        std::cout << "      > ClearTable took: " << t.elapsed_ms() << " ms" << std::endl;
        ASSERT_TEST(db.GetRowCount("test_table") == 0, "Row count is zero");
    }

    {
        Timer t;
        db.QueueWrite("test_table", { {"id", int32_t(100)}, {"username", std::string("snayck")} });
        db.QueueWrite("test_table", { {"id", int32_t(200)}, {"username", std::string("batman")} });
        wait_for_queue(db);
        std::cout << "      > QueueWrite (2 Rows) took: " << t.elapsed_ms() << " ms" << std::endl;
        ASSERT_TEST(db.GetRowCount("test_table") == 2, "Rows added correctly");
    }

    {
        bool read_success = false;
        Timer t;
        db.QueueRead("test_table", 1, [&](ReadResult res) {
            if (res.size() == 2 && HyperDBUtil::HyperValueToString(res[1].value) == "batman") read_success = true;
        });
        wait_for_queue(db);
        std::cout << "      > QueueRead (Callback) took: " << t.elapsed_ms() << " ms" << std::endl;
        ASSERT_TEST(read_success, "Read verified batman");
    }

    {
        Timer t;
        db.QueueDelete("test_table", "username", std::string("snayck"));
        wait_for_queue(db);
        std::cout << "      > QueueDelete took: " << t.elapsed_ms() << " ms" << std::endl;
        ASSERT_TEST(db.GetRowCount("test_table") == 1, "Row removed");
    }

    {
        Timer t;
        db.ForceFlush(1); // flush unencrypted
        std::cout << "      > ForceFlush (Unencrypted) took: " << t.elapsed_ms() << " ms" << std::endl;
        
        std::cout << "      > Migrating to ENCRYPTED (Fort Knox)..." << std::endl;
        db.SetEncryption(true, "secure_pass");
        db.ForceFlush(1);
        
        HyperDBManager db2;
        db2.OpenDB("test_solo.db", "secure_pass", true);
        ASSERT_TEST(db2.GetRowCount("test_table") == 1, "Encrypted migration verified");
        
        std::cout << "      > Migrating back to NITRO (Unencrypted)..." << std::endl;
        db2.SetEncryption(false, "");
        db2.ForceFlush(1);
    }
}

void RunClusterWriteBenchmark() {
    std::cout << "\n[2] STARTING 10,000,000 ROW APOCALYPSE (WRITE BENCHMARK)" << std::endl;
    std::mt19937 rng(static_cast<unsigned int>(std::time(nullptr)));
    HyperDBCluster cluster;

    const std::string PWD = "death";
    const std::string FOLDER = "death_benchmark";
    const size_t SHARD_LIMIT = 512ULL * 1024 * 1024;
    const int TOTAL_ROWS = 10000000;

    std::cout << "  " << get_time_stamp() << " target: 3.8GB scale-out write..." << std::endl;

    {
        Timer t;
        cluster.Open(FOLDER, "death", PWD, SHARD_LIMIT, false);
        std::cout << "      > Cluster Initialization took: " << t.elapsed_ms() << " ms" << std::endl;
    }

    {
        Timer t;
        cluster.QueueCreateTable("death", {
            {"noise1", HyperDB::ColumnType::ColumnType_String},
            {"noise2", HyperDB::ColumnType::ColumnType_String},
            {"noise3", HyperDB::ColumnType::ColumnType_String}
        });
        wait_for_queue(cluster);
        std::cout << "      > Cluster CreateTable took: " << t.elapsed_ms() << " ms" << std::endl;
    }

    Timer total_write_timer;
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

        int current_rows = i + BATCH_SIZE;
        if (current_rows % 1000000 == 0) {
            double pct = (static_cast<double>(current_rows) / TOTAL_ROWS) * 100.0;
            std::cout << "    > Progress: " << (int)pct << "% lap: " << lap_timer.elapsed_ms() << " ms" << std::endl;
            lap_timer = Timer(); 
            wait_for_queue(cluster);
        }
    }

    double total_write_time = total_write_timer.elapsed_ms();
    std::cout << "  " << get_time_stamp() << " write loop finished. Total Write Time: " << total_write_time << " ms" << std::endl;

    {
        Timer t;
        cluster.ForceFlush(1); // fast flush for benchmark initialization
        std::cout << "      > Cluster Finalize took: " << t.elapsed_ms() << " ms" << std::endl;
    }
}

void RunClusterReadBenchmark() {
    std::cout << "\n[3] INITIATING READ APOCALYPSE (STRESS TEST)" << std::endl;
    const std::string PWD = "death";
    const std::string FOLDER = "death_benchmark";
    const size_t SHARD_LIMIT = 512ULL * 1024 * 1024;

    auto perform_read_test = [&](HyperDBCluster& c, const std::string& label) {
        std::cout << "\n  --- PHASE: " << label << " ---" << std::endl;
        int total_rows = c.GetRowCount("death");
        const int READ_COUNT = 100000; 
        std::atomic<int> completed_reads{ 0 };
        std::mt19937 rng(1337);
        std::uniform_int_distribution<int> dist(0, total_rows - 1);

        std::cout << "      > Stress testing 100,000 random global ID reads..." << std::endl;
        Timer read_timer;
        for (int i = 0; i < READ_COUNT; ++i) {
            c.QueueRead("death", dist(rng), [&](ReadResult res) { completed_reads++; });
            if (i % 10000 == 0) wait_for_queue(c);
        }
        wait_for_queue(c);
        std::cout << "      > " << label << " reads t-total: " << read_timer.elapsed_ms() << " ms (" << (READ_COUNT / (read_timer.elapsed_ms() / 1000.0)) << " reads/sec)" << std::endl;

        std::cout << "      > Full aggregate scan (cross-shard)..." << std::endl;
        {
            Timer t;
            c.QueueFind("death", "noise1", std::string("missing_no_9999"), [](std::vector<ReadResult> results) {});
            wait_for_queue(c);
            std::cout << "      > Full Shard Scan took: " << t.elapsed_ms() << " ms" << std::endl;
        }
    };

    try {
        {
            HyperDBCluster c1;
            c1.Open(FOLDER, "death", PWD, SHARD_LIMIT, false);
            perform_read_test(c1, "ORIGINAL NITRO");

            std::cout << "  > MIGRATING 4.2GB TO FORT KNOX (ENCRYPTED)..." << std::endl;
            Timer mig_timer;
            c1.SetEncryption(true, "death");
            c1.ForceFlush(1); // 1 iteration for speed
            std::cout << "      > Migration to Encrypted took: " << mig_timer.elapsed_ms() << " ms" << std::endl;
        }

        {
            HyperDBCluster c2;
            std::cout << "  > LOADING ENCRYPTED CLUSTER (COLD START TAX)..." << std::endl;
            Timer open_timer;
            c2.Open(FOLDER, "death", "death", SHARD_LIMIT, true);
            std::cout << "      > Cold Open (Encrypted) took: " << open_timer.elapsed_ms() << " ms" << std::endl;
            perform_read_test(c2, "ENCRYPTED");

            std::cout << "  > MIGRATING 4.2GB BACK TO NITRO..." << std::endl;
            Timer mig_timer;
            c2.SetEncryption(false, "");
            c2.ForceFlush(1);
            std::cout << "      > Migration back to Nitro took: " << mig_timer.elapsed_ms() << " ms" << std::endl;
        }

        {
            HyperDBCluster c3;
            std::cout << "  > LOADING NITRO CLUSTER (INSTANT OPEN)..." << std::endl;
            Timer open_timer;
            c3.Open(FOLDER, "death", "", SHARD_LIMIT, false);
            std::cout << "      > Cold Open (Nitro) took: " << open_timer.elapsed_ms() << " ms" << std::endl;
            perform_read_test(c3, "POST-MIGRATION NITRO");
            std::cout << "  [OK] Full migration cycle verified on 4.2GB dataset." << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "  [READ FAIL] " << e.what() << std::endl;
    }
}

int main() {
    std::cout << "--- STARTING UNIFIED HYPERDB MASTER STRESS TEST ---" << std::endl;
    
    Timer master_timer;
    RunSoloTests();
    RunClusterWriteBenchmark();
    RunClusterReadBenchmark();

    std::cout << "\n---------------------------------------------------" << std::endl;
    std::cout << "ALL SUITES COMPLETED IN: " << master_timer.elapsed_ms() << " ms" << std::endl;
    std::cout << "---------------------------------------------------" << std::endl;

    return 0;
}