// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "Helper/Concurrent.h"

SPTAG::Helper::Concurrent::WaitSignal::WaitSignal()
    : m_isWaiting(false),
      m_unfinished(0) {
}

SPTAG::Helper::Concurrent::WaitSignal::WaitSignal(std::uint32_t p_unfinished)
    : m_isWaiting(false),
      m_unfinished(p_unfinished) {
}

SPTAG::Helper::Concurrent::WaitSignal::~WaitSignal() {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (m_isWaiting) {
        m_cv.notify_all();
    }
}

void SPTAG::Helper::Concurrent::WaitSignal::Reset(std::uint32_t p_unfinished) {
    std::lock_guard<std::mutex> guard(m_mutex);
    if (m_isWaiting) {
        m_cv.notify_all();
    }

    m_isWaiting = false;
    m_unfinished = p_unfinished;
}

void SPTAG::Helper::Concurrent::WaitSignal::Wait() {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_unfinished > 0) {
        m_isWaiting = true;
        m_cv.wait(lock);
    }
}

void SPTAG::Helper::Concurrent::WaitSignal::FinishOne() {
    if (1 == m_unfinished.fetch_sub(1)) {
        std::lock_guard<std::mutex> guard(m_mutex);
        if (m_isWaiting) {
            m_isWaiting = false;
            m_cv.notify_all();
        }
    }
}
