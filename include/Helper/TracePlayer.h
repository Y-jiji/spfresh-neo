// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_TRACEPLAYER_H_
#define _SPTAG_HELPER_TRACEPLAYER_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace SPTAG::Helper {

enum class OperationKind : std::uint8_t {
    Read = 0,
    Write = 1
};

template <typename T>
class TracePlayer;

template <typename T>
struct TraceRecord {
    const T* m_data;
    std::size_t m_dim;
    std::size_t m_seqNum;
    OperationKind m_op;

    OperationKind GetOperationKind() const;
    const T* Data() const;
    std::size_t Dimension() const;
    std::size_t SequenceNumber() const;
};

template <typename T>
class TraceRecordGuard {
   public:
    TraceRecordGuard();
    TraceRecordGuard(TracePlayer<T>* p_player, std::size_t p_slot, TraceRecord<T> p_record);
    ~TraceRecordGuard();

    // Move-only semantics
    TraceRecordGuard(TraceRecordGuard&& p_other) noexcept;
    TraceRecordGuard& operator=(TraceRecordGuard&& p_other) noexcept;
    TraceRecordGuard(const TraceRecordGuard&) = delete;
    TraceRecordGuard& operator=(const TraceRecordGuard&) = delete;

    const TraceRecord<T>& operator*() const;
    const TraceRecord<T>* operator->() const;
    bool Valid() const;

   private:
    void Release();

    TracePlayer<T>* m_player;
    std::size_t m_slot;
    TraceRecord<T> m_record;
};

template <typename T>
class TracePlayer {
   public:
    using HashFunction = std::function<std::uint64_t(std::uint64_t)>;

    TracePlayer(const std::string& p_filepath, std::size_t p_windowSize, HashFunction p_hashFn);
    ~TracePlayer();

    // Non-copyable, non-movable
    TracePlayer(const TracePlayer&) = delete;
    TracePlayer& operator=(const TracePlayer&) = delete;
    TracePlayer(TracePlayer&&) = delete;
    TracePlayer& operator=(TracePlayer&&) = delete;

    std::optional<TraceRecordGuard<T>> Next();

    std::size_t GetDimension() const;
    std::size_t GetTotalVectors() const;
    std::size_t GetWindowSize() const;

   private:
    friend class TraceRecordGuard<T>;

    void ReleaseSlot(std::size_t p_slot);
    void LoadIntoSlot(std::size_t p_slot, std::size_t p_seqNum);
    OperationKind DetermineOp(std::size_t p_seqNum) const;
    void StartPrefetcher();
    void StopPrefetcher();
    void PrefetcherLoop();

    // File handle
    int m_fd;
    std::size_t m_totalVectors;
    std::size_t m_dim;
    std::size_t m_vectorBytes;
    std::size_t m_headerBytes;

    // Sliding window ring buffer
    std::size_t m_windowSize;
    std::vector<T> m_buffer;  // m_windowSize * m_dim elements

    // Double buffer for prefetching
    std::vector<T> m_prefetchBuffer;
    std::atomic<std::size_t> m_prefetchedUpTo{0};
    std::atomic<bool> m_prefetcherRunning{false};
    std::thread m_prefetcherThread;

    // Per-slot reference counts (lock-free)
    // Using unique_ptr array because std::atomic is not copyable/movable
    std::unique_ptr<std::atomic<std::uint32_t>[]> m_slotRefs;

    // Sequence tracking
    std::atomic<std::size_t> m_nextSeq{0};

    // Hash function for operation kind
    HashFunction m_hashFn;
};

// =============================================================================
// TraceRecord Implementation
// =============================================================================

template <typename T>
OperationKind TraceRecord<T>::GetOperationKind() const {
    return m_op;
}

template <typename T>
const T* TraceRecord<T>::Data() const {
    return m_data;
}

template <typename T>
std::size_t TraceRecord<T>::Dimension() const {
    return m_dim;
}

template <typename T>
std::size_t TraceRecord<T>::SequenceNumber() const {
    return m_seqNum;
}

// =============================================================================
// TraceRecordGuard Implementation
// =============================================================================

template <typename T>
TraceRecordGuard<T>::TraceRecordGuard()
    : m_player(nullptr), m_slot(0), m_record{nullptr, 0, 0, OperationKind::Read} {}

template <typename T>
TraceRecordGuard<T>::TraceRecordGuard(TracePlayer<T>* p_player, std::size_t p_slot, TraceRecord<T> p_record)
    : m_player(p_player), m_slot(p_slot), m_record(p_record) {}

template <typename T>
TraceRecordGuard<T>::~TraceRecordGuard() {
    Release();
}

template <typename T>
TraceRecordGuard<T>::TraceRecordGuard(TraceRecordGuard&& p_other) noexcept
    : m_player(p_other.m_player), m_slot(p_other.m_slot), m_record(p_other.m_record) {
    p_other.m_player = nullptr;
}

template <typename T>
TraceRecordGuard<T>& TraceRecordGuard<T>::operator=(TraceRecordGuard&& p_other) noexcept {
    if (this != &p_other) {
        Release();
        m_player = p_other.m_player;
        m_slot = p_other.m_slot;
        m_record = p_other.m_record;
        p_other.m_player = nullptr;
    }
    return *this;
}

template <typename T>
void TraceRecordGuard<T>::Release() {
    if (m_player != nullptr) {
        m_player->ReleaseSlot(m_slot);
        m_player = nullptr;
    }
}

template <typename T>
const TraceRecord<T>& TraceRecordGuard<T>::operator*() const {
    return m_record;
}

template <typename T>
const TraceRecord<T>* TraceRecordGuard<T>::operator->() const {
    return &m_record;
}

template <typename T>
bool TraceRecordGuard<T>::Valid() const {
    return m_player != nullptr;
}

// =============================================================================
// TracePlayer Implementation
// =============================================================================

template <typename T>
TracePlayer<T>::TracePlayer(const std::string& p_filepath, std::size_t p_windowSize, HashFunction p_hashFn)
    : m_windowSize(p_windowSize), m_hashFn(std::move(p_hashFn)) {
    m_fd = open(p_filepath.c_str(), O_RDONLY);
    if (m_fd < 0) {
        throw std::runtime_error("TracePlayer: Failed to open file: " + p_filepath);
    }

    // Read header: (size, dim) as two uint32_t values (standard binary vector format)
    std::uint32_t size, dim;
    if (pread(m_fd, &size, sizeof(size), 0) != sizeof(size) ||
        pread(m_fd, &dim, sizeof(dim), sizeof(size)) != sizeof(dim)) {
        close(m_fd);
        throw std::runtime_error("TracePlayer: Failed to read file header");
    }

    m_totalVectors = size;
    m_dim = dim;
    m_vectorBytes = m_dim * sizeof(T);
    m_headerBytes = 2 * sizeof(std::uint32_t);

    // Allocate ring buffer
    m_buffer.resize(m_windowSize * m_dim);
    m_slotRefs = std::make_unique<std::atomic<std::uint32_t>[]>(m_windowSize);
    for (std::size_t i = 0; i < m_windowSize; ++i) {
        m_slotRefs[i].store(0, std::memory_order_relaxed);
    }

    // Allocate prefetch buffer (double buffer)
    m_prefetchBuffer.resize(m_windowSize * m_dim);
    m_prefetchedUpTo.store(0, std::memory_order_relaxed);

    // Start background prefetcher
    StartPrefetcher();
}

template <typename T>
TracePlayer<T>::~TracePlayer() {
    StopPrefetcher();
    if (m_fd >= 0) {
        close(m_fd);
    }
}

template <typename T>
void TracePlayer<T>::StartPrefetcher() {
    m_prefetcherRunning.store(true, std::memory_order_release);
    m_prefetcherThread = std::thread(&TracePlayer<T>::PrefetcherLoop, this);
}

template <typename T>
void TracePlayer<T>::StopPrefetcher() {
    m_prefetcherRunning.store(false, std::memory_order_release);
    if (m_prefetcherThread.joinable()) {
        m_prefetcherThread.join();
    }
}

template <typename T>
void TracePlayer<T>::PrefetcherLoop() {
    std::size_t prefetchSeq = 0;

    while (m_prefetcherRunning.load(std::memory_order_acquire)) {
        std::size_t currentNext = m_nextSeq.load(std::memory_order_acquire);

        // Prefetch up to windowSize ahead of current consumption
        std::size_t targetPrefetch = currentNext + m_windowSize;
        if (targetPrefetch > m_totalVectors) {
            targetPrefetch = m_totalVectors;
        }

        while (prefetchSeq < targetPrefetch) {
            std::size_t slot = prefetchSeq % m_windowSize;
            off_t offset = static_cast<off_t>(m_headerBytes + prefetchSeq * m_vectorBytes);
            T* dest = m_prefetchBuffer.data() + slot * m_dim;

            ssize_t bytesRead = pread(m_fd, dest, m_vectorBytes, offset);
            if (bytesRead != static_cast<ssize_t>(m_vectorBytes)) {
                // IO error, retry or break
                std::this_thread::yield();
                continue;
            }

            ++prefetchSeq;
            m_prefetchedUpTo.store(prefetchSeq, std::memory_order_release);
        }

        // Yield to avoid busy spinning when caught up
        std::this_thread::yield();
    }
}

template <typename T>
void TracePlayer<T>::ReleaseSlot(std::size_t p_slot) {
    m_slotRefs[p_slot].fetch_sub(1, std::memory_order_release);
}

template <typename T>
void TracePlayer<T>::LoadIntoSlot(std::size_t p_slot, std::size_t p_seqNum) {
    // Check if prefetcher has this data ready
    std::size_t prefetched = m_prefetchedUpTo.load(std::memory_order_acquire);

    if (p_seqNum < prefetched) {
        // Copy from prefetch buffer
        T* src = m_prefetchBuffer.data() + p_slot * m_dim;
        T* dest = m_buffer.data() + p_slot * m_dim;
        std::memcpy(dest, src, m_vectorBytes);
    } else {
        // Fallback: direct read (prefetcher hasn't caught up)
        off_t offset = static_cast<off_t>(m_headerBytes + p_seqNum * m_vectorBytes);
        T* dest = m_buffer.data() + p_slot * m_dim;

        ssize_t bytesRead = pread(m_fd, dest, m_vectorBytes, offset);
        if (bytesRead != static_cast<ssize_t>(m_vectorBytes)) {
            throw std::runtime_error("TracePlayer: Failed to read vector from file");
        }
    }
}

template <typename T>
OperationKind TracePlayer<T>::DetermineOp(std::size_t p_seqNum) const {
    std::uint64_t hash = m_hashFn(static_cast<std::uint64_t>(p_seqNum));
    return (hash & 1) ? OperationKind::Write : OperationKind::Read;
}

template <typename T>
std::optional<TraceRecordGuard<T>> TracePlayer<T>::Next() {
    // Atomically claim the next sequence number
    std::size_t seq = m_nextSeq.load(std::memory_order_acquire);

    // Check if trace is exhausted
    if (seq >= m_totalVectors) {
        return std::nullopt;
    }

    // CAS loop to claim this sequence number
    while (!m_nextSeq.compare_exchange_weak(seq, seq + 1,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
        if (seq >= m_totalVectors) {
            return std::nullopt;
        }
    }

    std::size_t slot = seq % m_windowSize;

    // Atomically acquire the slot: spin-wait until slot is free (ref_count == 0)
    // and then claim it by setting ref_count to 1 in a single CAS operation.
    // This prevents race conditions where multiple threads try to use the same slot.
    std::uint32_t expected = 0;
    while (!m_slotRefs[slot].compare_exchange_weak(expected, 1,
                                                    std::memory_order_acq_rel,
                                                    std::memory_order_acquire)) {
        expected = 0;  // Reset expected for next CAS attempt
        std::this_thread::yield();
    }

    // Now we exclusively own the slot with ref_count = 1
    // Load vector data into the slot
    LoadIntoSlot(slot, seq);

    // ref_count is already set to 1, guard will decrement when dropped

    TraceRecord<T> record{
        m_buffer.data() + slot * m_dim,
        m_dim,
        seq,
        DetermineOp(seq)};

    return TraceRecordGuard<T>(this, slot, record);
}

template <typename T>
std::size_t TracePlayer<T>::GetDimension() const {
    return m_dim;
}

template <typename T>
std::size_t TracePlayer<T>::GetTotalVectors() const {
    return m_totalVectors;
}

template <typename T>
std::size_t TracePlayer<T>::GetWindowSize() const {
    return m_windowSize;
}

}  // namespace SPTAG::Helper

#endif  // _SPTAG_HELPER_TRACEPLAYER_H_
