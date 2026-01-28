// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/ResultWriter.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <vector>

using namespace SPTAG::Helper;

// Test 1: Basic write and read back
bool TestBasicWriteRead() {
    std::cout << "  Testing Basic Write/Read..." << std::endl;

    const std::string testFile = "/tmp/result_writer_basic_test.bin";
    const std::size_t k = 10;
    const std::size_t numWriteRecords = 100;
    const std::size_t numReadRecords = 100;

    // Write records
    {
        ResultWriter writer(testFile, k);

        for (std::size_t i = 0; i < numWriteRecords; ++i) {
            writer.WriteInsertRecord(i, i * 1000);
        }

        std::vector<std::uint64_t> resultIds(k);
        for (std::size_t i = 0; i < numReadRecords; ++i) {
            for (std::size_t j = 0; j < k; ++j) {
                resultIds[j] = i * 100 + j;
            }
            writer.WriteSearchRecord(numWriteRecords + i, resultIds.data());
        }

        writer.Close();
    }

    // Read back and verify
    {
        ResultReader reader(testFile);

        if (reader.GetK() != k) {
            std::cerr << "    FAILED: K mismatch, expected " << k << ", got " << reader.GetK() << std::endl;
            return false;
        }

        std::size_t writeCount = 0, readCount = 0;
        ResultReader::Record record;

        while (reader.Next(record)) {
            if (record.m_type == ResultRecordType::Write) {
                if (record.m_seqNum != writeCount || record.m_internalId != writeCount * 1000) {
                    std::cerr << "    FAILED: Write record mismatch at " << writeCount << std::endl;
                    return false;
                }
                ++writeCount;
            } else {
                std::size_t expectedSeq = numWriteRecords + readCount;
                if (record.m_seqNum != expectedSeq) {
                    std::cerr << "    FAILED: Read record seqNum mismatch at " << readCount << std::endl;
                    return false;
                }
                for (std::size_t j = 0; j < k; ++j) {
                    if (record.m_resultIds[j] != readCount * 100 + j) {
                        std::cerr << "    FAILED: Read record resultId mismatch at " << readCount << std::endl;
                        return false;
                    }
                }
                ++readCount;
            }
        }

        if (writeCount != numWriteRecords || readCount != numReadRecords) {
            std::cerr << "    FAILED: Record count mismatch" << std::endl;
            return false;
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: Basic write/read verified" << std::endl;
    return true;
}

// Test 2: Multi-threaded writes - all records written exactly once
bool TestMultiThreadedWrites() {
    std::cout << "  Testing Multi-threaded Writes..." << std::endl;

    const std::string testFile = "/tmp/result_writer_mt_test.bin";
    const std::size_t k = 5;
    const std::size_t numRecordsPerThread = 1000;
    const int numThreads = 8;
    const std::size_t totalRecords = numRecordsPerThread * numThreads;

    {
        ResultWriter writer(testFile, k, 1024);

        std::atomic<std::size_t> counter{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&, t]() {
                std::vector<std::uint64_t> resultIds(k);

                for (std::size_t i = 0; i < numRecordsPerThread; ++i) {
                    std::size_t seqNum = counter.fetch_add(1, std::memory_order_relaxed);

                    // Alternate between write and read records
                    if (seqNum % 2 == 0) {
                        writer.WriteInsertRecord(seqNum, seqNum * 10);
                    } else {
                        for (std::size_t j = 0; j < k; ++j) {
                            resultIds[j] = seqNum * 100 + j;
                        }
                        writer.WriteSearchRecord(seqNum, resultIds.data());
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        writer.Close();
    }

    // Read back and verify
    {
        ResultReader reader(testFile);
        std::set<std::uint64_t> seenSeqNums;
        ResultReader::Record record;

        while (reader.Next(record)) {
            seenSeqNums.insert(record.m_seqNum);

            // Verify data integrity
            if (record.m_type == ResultRecordType::Write) {
                if (record.m_internalId != record.m_seqNum * 10) {
                    std::cerr << "    FAILED: Write record data mismatch at seq " << record.m_seqNum << std::endl;
                    return false;
                }
            } else {
                for (std::size_t j = 0; j < k; ++j) {
                    if (record.m_resultIds[j] != record.m_seqNum * 100 + j) {
                        std::cerr << "    FAILED: Read record data mismatch at seq " << record.m_seqNum << std::endl;
                        return false;
                    }
                }
            }
        }

        if (seenSeqNums.size() != totalRecords) {
            std::cerr << "    FAILED: Expected " << totalRecords << " records, got " << seenSeqNums.size() << std::endl;
            return false;
        }

        // Verify all sequence numbers present
        for (std::size_t i = 0; i < totalRecords; ++i) {
            if (seenSeqNums.find(i) == seenSeqNums.end()) {
                std::cerr << "    FAILED: Missing sequence number " << i << std::endl;
                return false;
            }
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: " << totalRecords << " records written correctly by " << numThreads << " threads" << std::endl;
    return true;
}

// Test 3: High contention stress test
bool TestHighContention() {
    std::cout << "  Testing High Contention..." << std::endl;

    const std::string testFile = "/tmp/result_writer_stress_test.bin";
    const std::size_t k = 20;
    const std::size_t numRecordsPerThread = 5000;
    const int numThreads = 16;
    const std::size_t numSlots = 64;  // Small buffer = high contention

    auto startTime = std::chrono::high_resolution_clock::now();

    {
        ResultWriter writer(testFile, k, numSlots);

        std::atomic<std::size_t> counter{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&]() {
                std::vector<std::uint64_t> resultIds(k);

                for (std::size_t i = 0; i < numRecordsPerThread; ++i) {
                    std::size_t seqNum = counter.fetch_add(1, std::memory_order_relaxed);

                    if (seqNum % 3 == 0) {
                        writer.WriteInsertRecord(seqNum, seqNum);
                    } else {
                        for (std::size_t j = 0; j < k; ++j) {
                            resultIds[j] = seqNum + j;
                        }
                        writer.WriteSearchRecord(seqNum, resultIds.data());
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        writer.Close();
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime);

    // Verify count
    {
        ResultReader reader(testFile);
        std::size_t count = 0;
        ResultReader::Record record;
        while (reader.Next(record)) {
            ++count;
        }

        std::size_t expected = numRecordsPerThread * numThreads;
        if (count != expected) {
            std::cerr << "    FAILED: Expected " << expected << " records, got " << count << std::endl;
            return false;
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: " << (numRecordsPerThread * numThreads) << " records with "
              << numThreads << " threads in " << duration.count() << "ms" << std::endl;
    return true;
}

// Test 4: Data integrity with large K
bool TestLargeK() {
    std::cout << "  Testing Large K Value..." << std::endl;

    const std::string testFile = "/tmp/result_writer_largek_test.bin";
    const std::size_t k = 100;
    const std::size_t numRecords = 1000;
    const int numThreads = 4;

    {
        ResultWriter writer(testFile, k);

        std::atomic<std::size_t> counter{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&]() {
                std::vector<std::uint64_t> resultIds(k);

                while (true) {
                    std::size_t seqNum = counter.fetch_add(1, std::memory_order_relaxed);
                    if (seqNum >= numRecords) break;

                    // Fill with deterministic pattern
                    for (std::size_t j = 0; j < k; ++j) {
                        resultIds[j] = seqNum * 1000 + j;
                    }
                    writer.WriteSearchRecord(seqNum, resultIds.data());
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        writer.Close();
    }

    // Verify
    {
        ResultReader reader(testFile);
        std::size_t count = 0;
        ResultReader::Record record;

        while (reader.Next(record)) {
            if (record.m_type != ResultRecordType::Read) {
                std::cerr << "    FAILED: Expected Read record" << std::endl;
                return false;
            }

            if (record.m_resultIds.size() != k) {
                std::cerr << "    FAILED: Result size mismatch" << std::endl;
                return false;
            }

            for (std::size_t j = 0; j < k; ++j) {
                if (record.m_resultIds[j] != record.m_seqNum * 1000 + j) {
                    std::cerr << "    FAILED: Data corruption at seq " << record.m_seqNum << std::endl;
                    return false;
                }
            }
            ++count;
        }

        if (count != numRecords) {
            std::cerr << "    FAILED: Expected " << numRecords << " records, got " << count << std::endl;
            return false;
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: Large K (" << k << ") with " << numRecords << " records verified" << std::endl;
    return true;
}

// Test 5: Flush behavior
bool TestFlushBehavior() {
    std::cout << "  Testing Flush Behavior..." << std::endl;

    const std::string testFile = "/tmp/result_writer_flush_test.bin";
    const std::size_t k = 5;

    {
        ResultWriter writer(testFile, k);

        // Write some records
        writer.WriteInsertRecord(0, 100);
        writer.WriteInsertRecord(1, 200);

        // Flush
        writer.Flush();

        // Write more
        std::vector<std::uint64_t> resultIds(k, 42);
        writer.WriteSearchRecord(2, resultIds.data());

        // Flush again
        writer.Flush();

        writer.Close();
    }

    // Verify
    {
        ResultReader reader(testFile);
        std::size_t count = 0;
        ResultReader::Record record;

        while (reader.Next(record)) {
            ++count;
        }

        if (count != 3) {
            std::cerr << "    FAILED: Expected 3 records after flushes, got " << count << std::endl;
            return false;
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: Flush behavior verified" << std::endl;
    return true;
}

// Test 6: Record type distribution
bool TestRecordTypeDistribution() {
    std::cout << "  Testing Record Type Distribution..." << std::endl;

    const std::string testFile = "/tmp/result_writer_dist_test.bin";
    const std::size_t k = 10;
    const std::size_t numWriteRecords = 3000;
    const std::size_t numReadRecords = 2000;
    const int numThreads = 8;

    {
        ResultWriter writer(testFile, k);

        std::atomic<std::size_t> writeCounter{0};
        std::atomic<std::size_t> readCounter{0};
        std::vector<std::thread> threads;

        for (int t = 0; t < numThreads; ++t) {
            threads.emplace_back([&]() {
                std::vector<std::uint64_t> resultIds(k);

                while (true) {
                    std::size_t wc = writeCounter.load(std::memory_order_relaxed);
                    std::size_t rc = readCounter.load(std::memory_order_relaxed);

                    if (wc >= numWriteRecords && rc >= numReadRecords) break;

                    // Try to do a write record
                    if (wc < numWriteRecords) {
                        std::size_t claimed = writeCounter.fetch_add(1, std::memory_order_relaxed);
                        if (claimed < numWriteRecords) {
                            writer.WriteInsertRecord(claimed, claimed * 10);
                            continue;
                        }
                    }

                    // Try to do a read record
                    if (rc < numReadRecords) {
                        std::size_t claimed = readCounter.fetch_add(1, std::memory_order_relaxed);
                        if (claimed < numReadRecords) {
                            for (std::size_t j = 0; j < k; ++j) {
                                resultIds[j] = claimed + j;
                            }
                            writer.WriteSearchRecord(claimed + 1000000, resultIds.data());
                        }
                    }
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        writer.Close();
    }

    // Verify distribution
    {
        ResultReader reader(testFile);
        std::size_t writeCount = 0, readCount = 0;
        ResultReader::Record record;

        while (reader.Next(record)) {
            if (record.m_type == ResultRecordType::Write) {
                ++writeCount;
            } else {
                ++readCount;
            }
        }

        if (writeCount != numWriteRecords) {
            std::cerr << "    FAILED: Expected " << numWriteRecords << " write records, got " << writeCount << std::endl;
            return false;
        }

        if (readCount != numReadRecords) {
            std::cerr << "    FAILED: Expected " << numReadRecords << " read records, got " << readCount << std::endl;
            return false;
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: Write=" << numWriteRecords << ", Read=" << numReadRecords << std::endl;
    return true;
}

// Test 7: Concurrent flush and write
bool TestConcurrentFlushAndWrite() {
    std::cout << "  Testing Concurrent Flush and Write..." << std::endl;

    const std::string testFile = "/tmp/result_writer_concurrent_flush_test.bin";
    const std::size_t k = 5;
    const std::size_t numRecords = 10000;
    const int numWriteThreads = 4;

    {
        ResultWriter writer(testFile, k, 256);

        std::atomic<std::size_t> counter{0};
        std::atomic<bool> done{false};
        std::vector<std::thread> threads;

        // Writer threads
        for (int t = 0; t < numWriteThreads; ++t) {
            threads.emplace_back([&]() {
                while (true) {
                    std::size_t seqNum = counter.fetch_add(1, std::memory_order_relaxed);
                    if (seqNum >= numRecords) break;
                    writer.WriteInsertRecord(seqNum, seqNum);
                }
            });
        }

        // Periodic flush thread
        threads.emplace_back([&]() {
            while (!done.load(std::memory_order_acquire)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                writer.Flush();
            }
        });

        // Wait for writers
        for (int t = 0; t < numWriteThreads; ++t) {
            threads[t].join();
        }

        done.store(true, std::memory_order_release);
        threads.back().join();

        writer.Close();
    }

    // Verify
    {
        ResultReader reader(testFile);
        std::set<std::uint64_t> seenSeqNums;
        ResultReader::Record record;

        while (reader.Next(record)) {
            seenSeqNums.insert(record.m_seqNum);
        }

        if (seenSeqNums.size() != numRecords) {
            std::cerr << "    FAILED: Expected " << numRecords << " records, got " << seenSeqNums.size() << std::endl;
            return false;
        }
    }

    std::remove(testFile.c_str());
    std::cout << "    PASSED: Concurrent flush with " << numRecords << " records" << std::endl;
    return true;
}

int main(int argc, char* argv[]) {
    std::cout << "======================================" << std::endl;
    std::cout << "ResultWriter Parallel Test Suite" << std::endl;
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

    runTest("BasicWriteRead", TestBasicWriteRead);
    runTest("MultiThreadedWrites", TestMultiThreadedWrites);
    runTest("HighContention", TestHighContention);
    runTest("LargeK", TestLargeK);
    runTest("FlushBehavior", TestFlushBehavior);
    runTest("RecordTypeDistribution", TestRecordTypeDistribution);
    runTest("ConcurrentFlushAndWrite", TestConcurrentFlushAndWrite);

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
