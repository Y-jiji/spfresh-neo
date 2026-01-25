// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_ASYNCFILEREADER_H_
#define _SPTAG_HELPER_ASYNCFILEREADER_H_

#include "Helper/DiskIO.h"
#include "Helper/ConcurrentSet.h"
#include "Core/Common.h"

#include <cstdint>
#include <functional>
#include <string>
#include <vector>
#include <thread>
#include <stdint.h>

#define ASYNC_READ 1
#define BATCH_READ 1

#define NUMA 1

#include <fcntl.h>
#include <sys/syscall.h>
#include <linux/aio_abi.h>
#ifdef NUMA
#include <numa.h>
#endif

namespace SPTAG
{
    namespace Helper
    {
        void SetThreadAffinity(int threadID, std::thread& thread, char socketStrategy = 0, char idStrategy = 0);
        extern struct timespec AIOTimeout;

        class RequestQueue
        {
        public:

            RequestQueue() :m_front(0), m_end(0), m_capacity(0) {}

            ~RequestQueue() {}

            void reset(int capacity) {
                if (capacity > m_capacity) {
                    m_capacity = capacity + 1;
                    m_queue.reset(new AsyncReadRequest * [m_capacity]);
                }
            }

            void push(AsyncReadRequest* j)
            {
                m_queue[m_end++] = j;
                if (m_end == m_capacity) m_end = 0;
            }

            bool pop(AsyncReadRequest*& j)
            {
                while (m_front == m_end) usleep(AIOTimeout.tv_nsec / 1000);
                j = m_queue[m_front++];
                if (m_front == m_capacity) m_front = 0;
                return true;
            }

            void* handle() 
            {
                return nullptr;
            }

        protected:
            int m_front, m_end, m_capacity;
            std::unique_ptr<AsyncReadRequest* []> m_queue;
        };

        class AsyncFileIO : public DiskIO
        {
        public:
            AsyncFileIO(DiskIOScenario scenario = DiskIOScenario::DIS_UserRead) {}

            virtual ~AsyncFileIO() { ShutDown(); }

            virtual bool Initialize(const char* filePath, int openMode,
                std::uint64_t maxIOSize = (1 << 20),
                std::uint32_t maxReadRetries = 2,
                std::uint32_t maxWriteRetries = 2,
                std::uint16_t threadPoolSize = 4)
            {
                m_fileHandle = open(filePath, O_RDONLY | O_DIRECT);
                if (m_fileHandle <= 0) {
                    LOG(LogLevel::LL_Error, "Failed to create file handle: %s\n", filePath);
                    return false;
                }

                m_iocps.resize(threadPoolSize);
                memset(m_iocps.data(), 0, sizeof(aio_context_t) * threadPoolSize);
                for (int i = 0; i < threadPoolSize; i++) {
                    auto ret = syscall(__NR_io_setup, (int)maxIOSize, &(m_iocps[i]));
                    if (ret < 0) {
                        LOG(LogLevel::LL_Error, "Cannot setup aio: %s\n", strerror(errno));
                        return false;
                    }
                }

#ifndef BATCH_READ
                m_shutdown = false;
                for (int i = 0; i < threadPoolSize; ++i)
                {
                    m_fileIocpThreads.emplace_back(std::thread(std::bind(&AsyncFileIO::ListionIOCP, this, i)));
                }
#endif
                return true;
            }

            virtual std::uint64_t ReadBinary(std::uint64_t readSize, char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return pread(m_fileHandle, (void*)buffer, readSize, offset);
            }

            virtual std::uint64_t WriteBinary(std::uint64_t writeSize, const char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint64_t ReadString(std::uint64_t& readSize, std::unique_ptr<char[]>& buffer, char delim = '\n', std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual std::uint64_t WriteString(const char* buffer, std::uint64_t offset = UINT64_MAX)
            {
                return 0;
            }

            virtual bool ReadFileAsync(AsyncReadRequest& readRequest)
            {
                struct iocb myiocb = { 0 };
                myiocb.aio_data = reinterpret_cast<uintptr_t>(&readRequest);
                myiocb.aio_lio_opcode = IOCB_CMD_PREAD;
                myiocb.aio_fildes = m_fileHandle;
                myiocb.aio_buf = (std::uint64_t)(readRequest.m_buffer);
                myiocb.aio_nbytes = readRequest.m_readSize;
                myiocb.aio_offset = static_cast<std::int64_t>(readRequest.m_offset);

                struct iocb* iocbs[1] = { &myiocb };
                int curTry = 0, maxTry = 10;
                while (curTry < maxTry && syscall(__NR_io_submit, m_iocps[(readRequest.m_status & 0xffff) % m_iocps.size()], 1, iocbs) < 1) {
                    usleep(AIOTimeout.tv_nsec / 1000);
                    curTry++;
                }
                if (curTry == maxTry) return false;
                return true;
            }

            virtual std::uint64_t TellP() { return 0; }

            virtual void ShutDown()
            {
                for (int i = 0; i < m_iocps.size(); i++) syscall(__NR_io_destroy, m_iocps[i]);
                close(m_fileHandle);
#ifndef BATCH_READ
                m_shutdown = true;
                for (auto& th : m_fileIocpThreads)
                {
                    if (th.joinable())
                    {
                        th.join();
                    }
                }
#endif
            }

            aio_context_t& GetIOCP(int i) { return m_iocps[i]; }

            int GetFileHandler() { return m_fileHandle; }

        private:
#ifndef BATCH_READ
            void ListionIOCP(int i) {
                int b = 10;
                std::vector<struct io_event> events(b);
                while (!m_shutdown)
                {
                    int numEvents = syscall(__NR_io_getevents, m_iocps[i], b, b, events.data(), &AIOTimeout);

                    for (int r = 0; r < numEvents; r++) {
                        AsyncReadRequest* req = reinterpret_cast<AsyncReadRequest*>((events[r].data));
                        if (nullptr != req)
                        {
                            req->m_callback(true);
                        }
                    }
                }
            }

            bool m_shutdown;

            std::vector<std::thread> m_fileIocpThreads;
#endif
            int m_fileHandle;

            std::vector<aio_context_t> m_iocps;
        };
        void BatchReadFileAsync(std::vector<std::shared_ptr<Helper::DiskIO>>& handlers, AsyncReadRequest* readRequests, int num);
    }
}

#endif // _SPTAG_HELPER_ASYNCFILEREADER_H_
