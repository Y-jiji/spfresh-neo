// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_RESULTWRITER_H_
#define _SPTAG_HELPER_RESULTWRITER_H_

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace SPTAG::Helper {

enum class ResultRecordType : std::uint8_t {
    Write = 0,  // seqNum (u64) + internalId (u64)
    Read = 1    // seqNum (u64) + k * resultId (u64)
};

class ResultWriter {
   public:
    ResultWriter(const std::string& p_filepath, std::size_t p_k, std::size_t p_numSlots = 4096);
    ~ResultWriter();

    // Non-copyable, non-movable
    ResultWriter(const ResultWriter&) = delete;
    ResultWriter& operator=(const ResultWriter&) = delete;
    ResultWriter(ResultWriter&&) = delete;
    ResultWriter& operator=(ResultWriter&&) = delete;

    // Lockless write methods - thread-safe, non-blocking (spins if buffer full)
    // p_seqNum: the sequence number of the trace record (for a insert)
    // p_internalId: the returned internal id by vector database
    void WriteInsertRecord(std::uint64_t p_seqNum, std::uint64_t p_internalId);
    // p_seqNum: the sequence number of the trace record (for a search)
    // p_resultIds: the returned internl ids of query result (sorted by distance incr)
    void WriteSearchRecord(std::uint64_t p_seqNum, const std::uint64_t* p_resultIds);

    // Flush all pending writes to disk
    void Flush();

    // Close the writer (flushes first)
    void Close();

    // Accessors
    std::size_t GetK() const;
    std::size_t GetNumSlots() const;
    std::size_t GetWriteRecordSize() const;
    std::size_t GetReadRecordSize() const;

   private:
    // Slot states
    static constexpr std::uint32_t SLOT_FREE = 0;
    static constexpr std::uint32_t SLOT_CLAIMED = 1;
    static constexpr std::uint32_t SLOT_READY = 2;

    struct alignas(64) Slot {  // Cache-line aligned to prevent false sharing
        std::atomic<std::uint32_t> m_status{SLOT_FREE};
        ResultRecordType m_type;
        std::uint8_t m_padding[3];
        // Data follows in m_slotData buffer
    };

    void StartFlusher();
    void StopFlusher();
    void FlusherLoop();
    std::size_t ClaimSlot();
    char* GetSlotData(std::size_t p_slotIdx);
    void MarkSlotReady(std::size_t p_slotIdx, ResultRecordType p_type);
    void WriteSlotToFile(std::size_t p_slotIdx);

    // File
    int m_fd;
    bool m_closed;

    // Parameters
    std::size_t m_k;
    std::size_t m_numSlots;
    std::size_t m_writeRecordSize;  // 1 + 2*8 bytes (type + seqNum + internalId)
    std::size_t m_readRecordSize;   // 1 + (1+k)*8 bytes (type + seqNum + k resultIds)
    std::size_t m_maxRecordSize;
    std::size_t m_slotDataSize;

    // Slot management
    std::unique_ptr<Slot[]> m_slots;
    std::unique_ptr<char[]> m_slotData;  // m_numSlots * m_slotDataSize

    // Position tracking
    std::atomic<std::size_t> m_claimPos{0};  // Next slot index to claim (mod m_numSlots)
    std::atomic<std::size_t> m_flushPos{0};  // Next slot index to flush (mod m_numSlots)

    // Flusher thread
    std::thread m_flusherThread;
    std::atomic<bool> m_flusherRunning{false};
    std::atomic<bool> m_flushRequested{false};

    // For synchronous flush
    std::mutex m_flushMutex;
    std::condition_variable m_flushCv;
    std::atomic<std::size_t> m_totalClaimed{0};
    std::atomic<std::size_t> m_totalFlushed{0};
};

// =============================================================================
// Implementation
// =============================================================================

inline ResultWriter::ResultWriter(const std::string& p_filepath, std::size_t p_k, std::size_t p_numSlots)
    : m_closed(false), m_k(p_k), m_numSlots(p_numSlots) {
    // Calculate record sizes
    // Write record: type (1 byte) + seqNum (8 bytes) + internalId (8 bytes) = 17 bytes
    m_writeRecordSize = 1 + 2 * sizeof(std::uint64_t);
    // Read record: type (1 byte) + seqNum (8 bytes) + k * resultId (k * 8 bytes)
    m_readRecordSize = 1 + (1 + m_k) * sizeof(std::uint64_t);
    m_maxRecordSize = (m_writeRecordSize > m_readRecordSize) ? m_writeRecordSize : m_readRecordSize;
    m_slotDataSize = m_maxRecordSize;

    // Open file
    m_fd = open(p_filepath.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (m_fd < 0) {
        throw std::runtime_error("ResultWriter: Failed to open file: " + p_filepath);
    }

    // Write header: k value
    std::uint64_t header = static_cast<std::uint64_t>(m_k);
    if (write(m_fd, &header, sizeof(header)) != sizeof(header)) {
        close(m_fd);
        throw std::runtime_error("ResultWriter: Failed to write header");
    }

    // Allocate slots
    m_slots = std::make_unique<Slot[]>(m_numSlots);
    m_slotData = std::make_unique<char[]>(m_numSlots * m_slotDataSize);

    // Initialize slots
    for (std::size_t i = 0; i < m_numSlots; ++i) {
        m_slots[i].m_status.store(SLOT_FREE, std::memory_order_relaxed);
    }

    // Start flusher thread
    StartFlusher();
}

inline ResultWriter::~ResultWriter() {
    if (!m_closed) {
        Close();
    }
}

inline void ResultWriter::StartFlusher() {
    m_flusherRunning.store(true, std::memory_order_release);
    m_flusherThread = std::thread(&ResultWriter::FlusherLoop, this);
}

inline void ResultWriter::StopFlusher() {
    m_flusherRunning.store(false, std::memory_order_release);
    m_flushRequested.store(true, std::memory_order_release);
    if (m_flusherThread.joinable()) {
        m_flusherThread.join();
    }
}

inline void ResultWriter::FlusherLoop() {
    while (m_flusherRunning.load(std::memory_order_acquire) ||
           m_totalFlushed.load(std::memory_order_acquire) < m_totalClaimed.load(std::memory_order_acquire)) {
        std::size_t flushIdx = m_flushPos.load(std::memory_order_acquire);
        std::size_t slotIdx = flushIdx % m_numSlots;

        // Check if slot is ready to flush
        if (m_slots[slotIdx].m_status.load(std::memory_order_acquire) == SLOT_READY) {
            // Write to file
            WriteSlotToFile(slotIdx);

            // Mark slot as free
            m_slots[slotIdx].m_status.store(SLOT_FREE, std::memory_order_release);

            // Advance flush position
            m_flushPos.fetch_add(1, std::memory_order_release);
            m_totalFlushed.fetch_add(1, std::memory_order_release);

            // Notify waiters
            m_flushCv.notify_all();
        } else {
            // No ready slot, yield
            std::this_thread::yield();
        }
    }
}

inline std::size_t ResultWriter::ClaimSlot() {
    std::size_t claimIdx = m_claimPos.fetch_add(1, std::memory_order_acq_rel);
    std::size_t slotIdx = claimIdx % m_numSlots;

    // Spin-wait until slot is free
    std::uint32_t expected = SLOT_FREE;
    while (!m_slots[slotIdx].m_status.compare_exchange_weak(
        expected, SLOT_CLAIMED,
        std::memory_order_acq_rel,
        std::memory_order_acquire)) {
        expected = SLOT_FREE;
        std::this_thread::yield();
    }

    m_totalClaimed.fetch_add(1, std::memory_order_release);
    return slotIdx;
}

inline char* ResultWriter::GetSlotData(std::size_t p_slotIdx) {
    return m_slotData.get() + p_slotIdx * m_slotDataSize;
}

inline void ResultWriter::MarkSlotReady(std::size_t p_slotIdx, ResultRecordType p_type) {
    m_slots[p_slotIdx].m_type = p_type;
    m_slots[p_slotIdx].m_status.store(SLOT_READY, std::memory_order_release);
}

inline void ResultWriter::WriteSlotToFile(std::size_t p_slotIdx) {
    char* data = GetSlotData(p_slotIdx);
    ResultRecordType type = m_slots[p_slotIdx].m_type;

    std::size_t recordSize = (type == ResultRecordType::Write) ? m_writeRecordSize : m_readRecordSize;

    ssize_t written = write(m_fd, data, recordSize);
    if (written != static_cast<ssize_t>(recordSize)) {
        // In production, handle error properly
        // For now, we'll ignore partial writes
    }
}

inline void ResultWriter::WriteInsertRecord(std::uint64_t p_seqNum, std::uint64_t p_internalId) {
    std::size_t slotIdx = ClaimSlot();
    char* data = GetSlotData(slotIdx);

    // Write record: type + seqNum + internalId
    data[0] = static_cast<char>(ResultRecordType::Write);
    std::memcpy(data + 1, &p_seqNum, sizeof(p_seqNum));
    std::memcpy(data + 1 + sizeof(p_seqNum), &p_internalId, sizeof(p_internalId));

    MarkSlotReady(slotIdx, ResultRecordType::Write);
}

inline void ResultWriter::WriteSearchRecord(std::uint64_t p_seqNum, const std::uint64_t* p_resultIds) {
    std::size_t slotIdx = ClaimSlot();
    char* data = GetSlotData(slotIdx);

    // Read record: type + seqNum + k resultIds
    data[0] = static_cast<char>(ResultRecordType::Read);
    std::memcpy(data + 1, &p_seqNum, sizeof(p_seqNum));
    std::memcpy(data + 1 + sizeof(p_seqNum), p_resultIds, m_k * sizeof(std::uint64_t));

    MarkSlotReady(slotIdx, ResultRecordType::Read);
}

inline void ResultWriter::Flush() {
    // Wait until all claimed slots have been flushed
    std::size_t targetFlushed = m_totalClaimed.load(std::memory_order_acquire);

    m_flushRequested.store(true, std::memory_order_release);

    std::unique_lock<std::mutex> lock(m_flushMutex);
    m_flushCv.wait(lock, [this, targetFlushed]() {
        return m_totalFlushed.load(std::memory_order_acquire) >= targetFlushed;
    });

    // Sync to disk
    fsync(m_fd);
}

inline void ResultWriter::Close() {
    if (m_closed) {
        return;
    }

    // Flush pending writes
    Flush();

    // Stop flusher thread
    StopFlusher();

    // Close file
    if (m_fd >= 0) {
        close(m_fd);
        m_fd = -1;
    }

    m_closed = true;
}

inline std::size_t ResultWriter::GetK() const {
    return m_k;
}

inline std::size_t ResultWriter::GetNumSlots() const {
    return m_numSlots;
}

inline std::size_t ResultWriter::GetWriteRecordSize() const {
    return m_writeRecordSize;
}

inline std::size_t ResultWriter::GetReadRecordSize() const {
    return m_readRecordSize;
}

// =============================================================================
// ResultReader - for reading back result files (useful for testing)
// =============================================================================

class ResultReader {
   public:
    struct Record {
        ResultRecordType m_type;
        std::uint64_t m_seqNum;
        std::uint64_t m_internalId;              // Only valid for Write records
        std::vector<std::uint64_t> m_resultIds;  // Only valid for Read records
    };

    ResultReader(const std::string& p_filepath);
    ~ResultReader();

    bool Next(Record& p_record);
    std::size_t GetK() const;
    void Reset();

   private:
    int m_fd;
    std::size_t m_k;
    std::size_t m_writeRecordSize;
    std::size_t m_readRecordSize;
    std::vector<char> m_buffer;
};

inline ResultReader::ResultReader(const std::string& p_filepath) {
    m_fd = open(p_filepath.c_str(), O_RDONLY);
    if (m_fd < 0) {
        throw std::runtime_error("ResultReader: Failed to open file: " + p_filepath);
    }

    // Read header
    std::uint64_t header;
    if (read(m_fd, &header, sizeof(header)) != sizeof(header)) {
        close(m_fd);
        throw std::runtime_error("ResultReader: Failed to read header");
    }
    m_k = static_cast<std::size_t>(header);

    m_writeRecordSize = 1 + 2 * sizeof(std::uint64_t);
    m_readRecordSize = 1 + (1 + m_k) * sizeof(std::uint64_t);

    std::size_t maxSize = (m_writeRecordSize > m_readRecordSize) ? m_writeRecordSize : m_readRecordSize;
    m_buffer.resize(maxSize);
}

inline ResultReader::~ResultReader() {
    if (m_fd >= 0) {
        close(m_fd);
    }
}

inline bool ResultReader::Next(Record& p_record) {
    // Read type byte
    char typeByte;
    ssize_t bytesRead = read(m_fd, &typeByte, 1);
    if (bytesRead <= 0) {
        return false;
    }

    p_record.m_type = static_cast<ResultRecordType>(typeByte);

    if (p_record.m_type == ResultRecordType::Write) {
        // Read seqNum + internalId
        std::uint64_t data[2];
        bytesRead = read(m_fd, data, sizeof(data));
        if (bytesRead != sizeof(data)) {
            return false;
        }
        p_record.m_seqNum = data[0];
        p_record.m_internalId = data[1];
        p_record.m_resultIds.clear();
    } else {
        // Read seqNum + k resultIds
        std::uint64_t seqNum;
        bytesRead = read(m_fd, &seqNum, sizeof(seqNum));
        if (bytesRead != sizeof(seqNum)) {
            return false;
        }
        p_record.m_seqNum = seqNum;
        p_record.m_internalId = 0;

        p_record.m_resultIds.resize(m_k);
        std::size_t resultBytes = m_k * sizeof(std::uint64_t);
        bytesRead = read(m_fd, p_record.m_resultIds.data(), resultBytes);
        if (bytesRead != static_cast<ssize_t>(resultBytes)) {
            return false;
        }
    }

    return true;
}

inline std::size_t ResultReader::GetK() const {
    return m_k;
}

inline void ResultReader::Reset() {
    lseek(m_fd, sizeof(std::uint64_t), SEEK_SET);  // Skip header
}

}  // namespace SPTAG::Helper

#endif  // _SPTAG_HELPER_RESULTWRITER_H_
