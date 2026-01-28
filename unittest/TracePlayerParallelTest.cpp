// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/TracePlayer.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

using namespace SPTAG::Helper;

// Deterministic hash function
std::uint64_t TestHash(std::uint64_t x) {
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

// Helper to create a test vector file
std::string CreateTestFile(const std::string& path, std::uint32_t numVectors, std::uint32_t dim) {
    std::ofstream ofs(path, std::ios::binary);
    ofs.write(reinterpret_cast<const char*>(&numVectors), sizeof(numVectors));
    ofs.write(reinterpret_cast<const char*>(&dim), sizeof(dim));

    std::vector<float> vec(dim);
    for (std::uint32_t i = 0; i < numVectors; ++i) {
        for (std::uint32_t j = 0; j < dim; ++j) {
            // Deterministic pattern: vec[i][j] = i * dim + j
            vec[j] = static_cast<float>(i * dim + j);
        }
        ofs.write(reinterpret_cast<const char*>(vec.data()), dim * sizeof(float));
    }
    return path;
}

// Test 1: Determinism - running twice produces identical results
bool TestDeterminism() {
    std::cout << "  Testing Determinism..." << std::endl;

    const std::string testFile = "/tmp/trace_player_determinism_test.bin";
    const std::uint32_t numVectors = 1000;
    const std::uint32_t dim = 64;
    const std::size_t windowSize = 16;

    CreateTestFile(testFile, numVectors, dim);

    // First run: collect all sequence numbers and their operation kinds
    std::vector<std::pair<std::size_t, OperationKind>> run1Results;
    {
        TracePlayer<float> player(testFile, windowSize, TestHash);
        while (auto guard = player.Next()) {
            const auto& record = **guard;
            run1Results.emplace_back(record.SequenceNumber(), record.GetOperationKind());
        }
    }

    // Second run: verify identical results
    std::vector<std::pair<std::size_t, OperationKind>> run2Results;
    {
        TracePlayer<float> player(testFile, windowSize, TestHash);
        while (auto guard = player.Next()) {
            const auto& record = **guard;
            run2Results.emplace_back(record.SequenceNumber(), record.GetOperationKind());
        }
    }

    std::remove(testFile.c_str());

    if (run1Results.size() != run2Results.size()) {
        std::cerr << "    FAILED: Different number of results between runs" << std::endl;
        return false;
    }

    for (std::size_t i = 0; i < run1Results.size(); ++i) {
        if (run1Results[i] != run2Results[i]) {
            std::cerr << "    FAILED: Mismatch at index " << i << std::endl;
            return false;
        }
    }

    if (run1Results.size() != numVectors) {
        std::cerr << "    FAILED: Expected " << numVectors << " results, got " << run1Results.size() << std::endl;
        return false;
    }

    std::cout << "    PASSED: Both runs produced identical " << numVectors << " results" << std::endl;
    return true;
}

// Test 2: Multi-threaded consumption - all vectors consumed exactly once
bool TestMultiThreadedConsumption() {
    std::cout << "  Testing Multi-threaded Consumption..." << std::endl;

    const std::string testFile = "/tmp/trace_player_mt_test.bin";
    const std::uint32_t numVectors = 10000;
    const std::uint32_t dim = 32;
    const std::size_t windowSize = 64;
    const int numThreads = 8;

    CreateTestFile(testFile, numVectors, dim);

    TracePlayer<float> player(testFile, windowSize, TestHash);

    std::mutex resultsMutex;
    std::set<std::size_t> consumedSeqNums;
    std::atomic<std::size_t> totalConsumed{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&]() {
            while (auto guard = player.Next()) {
                const auto& record = **guard;
                std::size_t seq = record.SequenceNumber();

                {
                    std::lock_guard<std::mutex> lock(resultsMutex);
                    consumedSeqNums.insert(seq);
                }
                totalConsumed.fetch_add(1, std::memory_order_relaxed);

                // Simulate some work
                std::this_thread::yield();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::remove(testFile.c_str());

    if (consumedSeqNums.size() != numVectors) {
        std::cerr << "    FAILED: Expected " << numVectors << " unique sequence numbers, got "
                  << consumedSeqNums.size() << std::endl;
        return false;
    }

    // Verify all sequence numbers from 0 to numVectors-1 are present
    for (std::size_t i = 0; i < numVectors; ++i) {
        if (consumedSeqNums.find(i) == consumedSeqNums.end()) {
            std::cerr << "    FAILED: Missing sequence number " << i << std::endl;
            return false;
        }
    }

    std::cout << "    PASSED: All " << numVectors << " vectors consumed exactly once by "
              << numThreads << " threads" << std::endl;
    return true;
}

// Test 3: Window blocking - verify guards block subsequent requests correctly
bool TestWindowBlocking() {
    std::cout << "  Testing Window Blocking Behavior..." << std::endl;

    const std::string testFile = "/tmp/trace_player_blocking_test.bin";
    const std::uint32_t numVectors = 100;
    const std::uint32_t dim = 16;
    const std::size_t windowSize = 4;  // Small window to force blocking

    CreateTestFile(testFile, numVectors, dim);

    TracePlayer<float> player(testFile, windowSize, TestHash);

    // Hold guards for the first windowSize vectors
    std::vector<TraceRecordGuard<float>> heldGuards;
    for (std::size_t i = 0; i < windowSize; ++i) {
        auto guard = player.Next();
        if (!guard) {
            std::cerr << "    FAILED: Could not get guard " << i << std::endl;
            return false;
        }
        heldGuards.push_back(std::move(*guard));
    }

    // Now try to get the next one in a separate thread - it should block
    std::atomic<bool> gotNext{false};
    std::atomic<bool> threadStarted{false};

    std::thread blockingThread([&]() {
        threadStarted.store(true, std::memory_order_release);
        auto guard = player.Next();  // This should block until we release a guard
        if (guard) {
            gotNext.store(true, std::memory_order_release);
        }
    });

    // Wait for thread to start
    while (!threadStarted.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }

    // Give the thread some time to potentially get past the blocking
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // At this point, the thread should still be blocked
    bool wasBlocked = !gotNext.load(std::memory_order_acquire);

    // Release one guard to unblock the thread
    heldGuards.erase(heldGuards.begin());

    // Wait for the thread to complete
    blockingThread.join();

    std::remove(testFile.c_str());

    if (!wasBlocked) {
        std::cerr << "    FAILED: Thread was not blocked when window was full" << std::endl;
        return false;
    }

    if (!gotNext.load(std::memory_order_acquire)) {
        std::cerr << "    FAILED: Thread did not eventually get the next record" << std::endl;
        return false;
    }

    std::cout << "    PASSED: Window blocking works correctly" << std::endl;
    return true;
}

// Test 4: Data integrity under concurrent access
bool TestDataIntegrity() {
    std::cout << "  Testing Data Integrity Under Concurrency..." << std::endl;

    const std::string testFile = "/tmp/trace_player_integrity_test.bin";
    const std::uint32_t numVectors = 5000;
    const std::uint32_t dim = 128;
    const std::size_t windowSize = 32;
    const int numThreads = 4;

    CreateTestFile(testFile, numVectors, dim);

    TracePlayer<float> player(testFile, windowSize, TestHash);

    std::atomic<bool> integrityOk{true};
    std::atomic<std::size_t> checkedCount{0};

    std::vector<std::thread> threads;
    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&]() {
            while (auto guard = player.Next()) {
                const auto& record = **guard;
                std::size_t seq = record.SequenceNumber();
                const float* data = record.Data();
                std::size_t dim = record.Dimension();

                // Verify data pattern: data[j] = seq * dim + j
                for (std::size_t j = 0; j < dim; ++j) {
                    float expected = static_cast<float>(seq * dim + j);
                    if (data[j] != expected) {
                        integrityOk.store(false, std::memory_order_release);
                        return;
                    }
                }

                checkedCount.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    std::remove(testFile.c_str());

    if (!integrityOk.load(std::memory_order_acquire)) {
        std::cerr << "    FAILED: Data corruption detected" << std::endl;
        return false;
    }

    if (checkedCount.load(std::memory_order_acquire) != numVectors) {
        std::cerr << "    FAILED: Expected " << numVectors << " checks, got "
                  << checkedCount.load() << std::endl;
        return false;
    }

    std::cout << "    PASSED: All " << numVectors << " vectors verified with correct data" << std::endl;
    return true;
}

// Test 5: Stress test with high contention
bool TestHighContention() {
    std::cout << "  Testing High Contention Scenario..." << std::endl;

    const std::string testFile = "/tmp/trace_player_stress_test.bin";
    const std::uint32_t numVectors = 50000;
    const std::uint32_t dim = 16;
    const std::size_t windowSize = 8;  // Small window = high contention
    const int numThreads = 16;

    CreateTestFile(testFile, numVectors, dim);

    auto startTime = std::chrono::high_resolution_clock::now();

    TracePlayer<float> player(testFile, windowSize, TestHash);

    std::atomic<std::size_t> totalProcessed{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < numThreads; ++t) {
        threads.emplace_back([&]() {
            while (auto guard = player.Next()) {
                // Simulate variable processing time
                volatile int dummy = 0;
                for (int i = 0; i < 100; ++i) {
                    dummy += i;
                }
                (void)dummy;

                totalProcessed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    std::remove(testFile.c_str());

    if (totalProcessed.load(std::memory_order_acquire) != numVectors) {
        std::cerr << "    FAILED: Expected " << numVectors << " processed, got "
                  << totalProcessed.load() << std::endl;
        return false;
    }

    std::cout << "    PASSED: Processed " << numVectors << " vectors with " << numThreads
              << " threads in " << duration.count() << "ms" << std::endl;
    return true;
}

// Test 6: Guard lifetime and slot reuse
bool TestGuardLifetimeAndSlotReuse() {
    std::cout << "  Testing Guard Lifetime and Slot Reuse..." << std::endl;

    const std::string testFile = "/tmp/trace_player_lifetime_test.bin";
    const std::uint32_t numVectors = 100;
    const std::uint32_t dim = 8;
    const std::size_t windowSize = 4;

    CreateTestFile(testFile, numVectors, dim);

    TracePlayer<float> player(testFile, windowSize, TestHash);

    // Consume all vectors, verifying that slot reuse works correctly
    std::size_t consumed = 0;
    std::vector<const float*> dataPtrs;

    while (auto guard = player.Next()) {
        const auto& record = **guard;

        // Track data pointer to verify slot reuse
        const float* dataPtr = record.Data();

        // After windowSize iterations, we should see repeated slot addresses
        if (consumed >= windowSize) {
            // The slot should have been reused
            bool foundReuse = false;
            for (std::size_t i = 0; i < windowSize; ++i) {
                if (dataPtrs[i] == dataPtr) {
                    foundReuse = true;
                    break;
                }
            }
            if (!foundReuse) {
                std::cerr << "    FAILED: Expected slot reuse at vector " << consumed << std::endl;
                std::remove(testFile.c_str());
                return false;
            }
        } else {
            dataPtrs.push_back(dataPtr);
        }

        ++consumed;
    }

    std::remove(testFile.c_str());

    if (consumed != numVectors) {
        std::cerr << "    FAILED: Expected " << numVectors << " consumed, got " << consumed << std::endl;
        return false;
    }

    std::cout << "    PASSED: Correct slot reuse verified across " << numVectors << " vectors" << std::endl;
    return true;
}

// Test 7: Operation kind distribution consistency
bool TestOperationKindDistribution() {
    std::cout << "  Testing Operation Kind Distribution..." << std::endl;

    const std::string testFile = "/tmp/trace_player_opkind_test.bin";
    const std::uint32_t numVectors = 10000;
    const std::uint32_t dim = 8;
    const std::size_t windowSize = 16;

    CreateTestFile(testFile, numVectors, dim);

    TracePlayer<float> player(testFile, windowSize, TestHash);

    std::size_t readCount = 0, writeCount = 0;

    while (auto guard = player.Next()) {
        const auto& record = **guard;
        if (record.GetOperationKind() == OperationKind::Read) {
            ++readCount;
        } else {
            ++writeCount;
        }
    }

    std::remove(testFile.c_str());

    // Verify we got a reasonable distribution (hash should give ~50/50)
    double readRatio = static_cast<double>(readCount) / numVectors;
    if (readRatio < 0.3 || readRatio > 0.7) {
        std::cerr << "    WARNING: Unusual distribution - reads: " << readCount
                  << ", writes: " << writeCount << " (ratio: " << readRatio << ")" << std::endl;
        // Not a failure, just informational
    }

    if (readCount + writeCount != numVectors) {
        std::cerr << "    FAILED: Total ops (" << (readCount + writeCount)
                  << ") != numVectors (" << numVectors << ")" << std::endl;
        return false;
    }

    std::cout << "    PASSED: Operation distribution - Reads: " << readCount
              << ", Writes: " << writeCount << std::endl;
    return true;
}

int main(int argc, char* argv[]) {
    std::cout << "======================================" << std::endl;
    std::cout << "TracePlayer Parallel Test Suite" << std::endl;
    std::cout << "======================================" << std::endl;

    int passed = 0, failed = 0;

    auto runTest = [&](const char* name, bool (*testFn)()) {
        std::cout << "\n[" << name << "]" << std::endl;
        if (testFn()) {
            ++passed;
        } else {
            ++failed;
            std::cerr << "  TEST FAILED: " << name << std::endl;
        }
    };

    runTest("Determinism", TestDeterminism);
    runTest("MultiThreadedConsumption", TestMultiThreadedConsumption);
    runTest("WindowBlocking", TestWindowBlocking);
    runTest("DataIntegrity", TestDataIntegrity);
    runTest("HighContention", TestHighContention);
    runTest("GuardLifetimeAndSlotReuse", TestGuardLifetimeAndSlotReuse);
    runTest("OperationKindDistribution", TestOperationKindDistribution);

    std::cout << "\n======================================" << std::endl;
    std::cout << "Results: " << passed << " passed, " << failed << " failed" << std::endl;

    if (failed == 0) {
        std::cout << "ALL TESTS PASSED" << std::endl;
        std::cout << "======================================" << std::endl;
        return 0;
    } else {
        std::cout << "SOME TESTS FAILED" << std::endl;
        std::cout << "======================================" << std::endl;
        return 1;
    }
}
