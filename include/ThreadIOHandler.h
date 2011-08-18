#ifndef __ThreadIOHandler__h__
#define __ThreadIOHandler__h__

#include "IOPlexer.h"
#include <pthread.h>
///////////////////////////////////////////////////////////////////////

class ThreadIOHandler: public IOHandler
{
        enum {
                DEFAULT_HASH_SIZE = 32768,
                READ_BUFFER_SIZE  = 131072,
                WRITE_BUFFER_SIZE = 131072
        };

protected:
        struct Chunk
        {
                Chunk*  prev;
                Chunk*  next;
                char*   buf;
                size_t  size;

                Chunk(const char* buf = NULL, size_t size = size_t(-1));

                ~Chunk();
        };

        class Buffer
        {
                pthread_mutex_t mutex;
                pthread_cond_t  readEvent;
                pthread_cond_t  writtenEvent;

                Chunk*          head;
                Chunk*          tail;
                Chunk*          cur;
                size_t          fense;
                size_t          offset;
                size_t          count;
                size_t          capacity;

#if ENABLE_STATISTICS
                uint32_t        &gCount;
                uint32_t        &gPeakCount;
                uint64_t        &gTotalCount;
                uint32_t        &gCapacity;
                uint32_t        &gPeakCapacity;
                uint64_t        &gTotalCapacity;
#endif
        public:
                Buffer(
#if ENABLE_STATISTICS
                        uint32_t &gCount,
                        uint32_t &gPeakCount,
                        uint64_t &gTotalCount,
                        uint32_t &gCapacity,
                        uint32_t &gPeakCapacity,
                        uint64_t &gTotalCapacity
#endif
                );

                ~Buffer();

                void
                clear();

                bool
                append(const char* buf, size_t size = size_t(-1));

                size_t
                fetch(char* buf, size_t size, bool perChunk = false);

                void
                rewind();

                void
                unwind(size_t size);

                void
                consume(size_t size);

                bool
                isEmpty();

                size_t
                getCapacity() const;

                size_t
                getCount() const;

                bool
                checkReadable(int milliseconds = 0);

                bool
                checkWritable(size_t maxCapacity, int milliseconds = 0);
        };

private:
        class BufferMap;

        struct EventSlot
        {
                EventSlot*        prev;
                EventSlot*        next;

                IOPlexer*         ioplexer;
                int               events;
                IOHandlerContext* ctx;

                EventSlot(IOPlexer* ioplexer, int events, IOHandlerContext* ctx);

                ~EventSlot();
        };

        struct BufferSlot
        {
                BufferSlot*     next;
                int             fd;
                bool            closed;
                Buffer          incoming;
                Buffer          outgoing;
                unsigned int    refcount;

                pthread_mutex_t mutex;
                pthread_cond_t  releasedEvent;
                pthread_t       occupied;
                unsigned int    reentrance;

                unsigned int    occurrence;
                EventSlot*      eventHead;
                EventSlot*      eventTail;

                BufferSlot(
#if ENABLE_STATISTICS
                        uint32_t &pendingCount,
                        uint32_t &incoming_size,
                        uint32_t &incoming_peakSize,
                        uint64_t &incoming_totalSize,
                        uint32_t &incoming_chunkCount,
                        uint32_t &incoming_peakChunkCount,
                        uint64_t &incoming_totalChunkCount,
                        uint32_t &outgoing_size,
                        uint32_t &outgoing_peakSize,
                        uint64_t &outgoing_totalSize,
                        uint32_t &outgoing_chunkCount,
                        uint32_t &outgoing_peakChunkCount,
                        uint64_t &outgoing_totalChunkCount
#endif
                );

                unsigned int
                addRef();

                unsigned int
                release();

                void
                lock();

                bool
                tryLock(int milliseconds = 0);

                void
                unlock();

                unsigned int
                addPendingEvent(
                        IOPlexer*         ioplexer,
                        int               events,
                        IOHandlerContext* ctx
                );

                unsigned int
                getPendingEvent(
                        IOPlexer*         &ioplexer,
                        int               &events,
                        IOHandlerContext* &ctx
                );

                void
                clearPendingEvents();

        private:
                ~BufferSlot();

#if ENABLE_STATISTICS
                uint32_t &pendingCount;
#endif
        };

        class BufferMap
        {
                pthread_mutex_t mutex;
                BufferSlot**    hashTable;
                size_t          hashSize;
                size_t          count;

#if ENABLE_STATISTICS
                uint32_t        pendingCount;
                uint32_t        peakCount;
                uint64_t        totalCount;
                uint32_t        incoming_size;
                uint32_t        incoming_peakSize;
                uint64_t        incoming_totalSize;
                uint32_t        incoming_chunkCount;
                uint32_t        incoming_peakChunkCount;
                uint64_t        incoming_totalChunkCount;
                uint32_t        outgoing_size;
                uint32_t        outgoing_peakSize;
                uint64_t        outgoing_totalSize;
                uint32_t        outgoing_chunkCount;
                uint32_t        outgoing_peakChunkCount;
                uint64_t        outgoing_totalChunkCount;
#endif
        public:
                BufferMap(int hashSize = -1);

                ~BufferMap();

                size_t
                getHashSize() const;

                size_t
                getCount() const;

#if ENABLE_STATISTICS
                void
                getStatistics(IOStatistics* statistics);
#endif
                void
                clear();

                BufferSlot*
                get(int fd);

                BufferSlot*
                acquire(int fd);

                void
                remove(int fd);

                size_t 
                hash(int fd);
        };

        struct Task
        {
                Task*             prev;
                Task*             next;
                IOPlexer*         ioplexer;
                int               fd;
                int               events;
                IOHandlerContext* ctx;
                BufferSlot*       slot;

                Task(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx, BufferSlot* slot);

                ~Task();
        };

        class TaskQueue
        {
                Task*           head;
                Task*           tail;
                size_t          size;
                bool            running;
#if ENABLE_STATISTICS
                uint32_t        peak;
                uint64_t        total;
#endif
                pthread_mutex_t mutex;
                pthread_cond_t  event;

        public:
                TaskQueue();

                ~TaskQueue();

                size_t
                getSize() const;

#if ENABLE_STATISTICS
                void
                getStatistics(IOStatistics* statistics);
#endif

                void
                clear();

                bool
                enqueue(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx, BufferSlot* slot);

                bool
                dequeue(IOPlexer* &ioplexer, int &fd, int &events, IOHandlerContext* &ctx, BufferSlot* &slot, int milliseconds = -1);

                bool
                reschedule(BufferSlot* slot);

                bool
                isRunning() const;

                void
                preNotifyStop();
        };

        typedef void (*ThreadRoutine)(void* ctx);

        struct Thread
        {
                Thread*        prev;
                Thread*        next;
                pthread_t      id;
                ThreadRoutine  routine;
                void*          ctx;
                unsigned int   refcount;

                Thread(ThreadRoutine routine = NULL, void* ctx = NULL);

                unsigned int
                addRef();

                unsigned int
                release();

        private:
                ~Thread();
        };

        struct ThreadContext
        {
                ThreadIOHandler* handler;
                Thread*          thread;
                unsigned int     refcount;

                ThreadContext(ThreadIOHandler* handler, Thread* thread);

                unsigned int
                addRef();

                unsigned int
                release();

        private:
                ~ThreadContext();
        };

        class ThreadPool
        {
                pthread_mutex_t mutex;
                Thread*         head;
                Thread*         tail;
                bool            running;
                size_t          count;
#if ENABLE_STATISTICS
                uint32_t        peak;
                uint64_t        total;
#endif
        public:
                ThreadPool();

                ~ThreadPool();

                size_t
                getCount() const;

#if ENABLE_STATISTICS
                void
                getStatistics(IOStatistics* s);
#endif
                bool
                isRunning() const;

                bool
                startThread(ThreadIOHandler* handler, ThreadRoutine routine, void* ctx);

                void
                preNotifyStop();

                void
                joinAllThreads();

        private:
                static void*
                threadRoutine(void* ctx);

                void
                addToList(Thread* thread);

                Thread*
                removeFromList(Thread* thread);
        };

        BufferMap  bufMap;
        TaskQueue  taskQueue;
        ThreadPool threadPool;
        int        minThreads;
        int        maxThreads;
        size_t     numActive;

#if ENABLE_STATISTICS
        uint32_t   peakActive;
        uint64_t   totalActive;
#endif

public:
        ThreadIOHandler(int minThreads = 0, int maxThreads = -1, int hashSize = -1);

        virtual
        ~ThreadIOHandler();

        virtual void
        shutdown();

#if ENABLE_STATISTICS
        virtual void
        getStatistics(IOStatistics* statistics);
#endif

protected:
        virtual void
        onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx, Buffer& incoming, Buffer& outgoing);

        virtual int
        onRecv(IOPlexer* ioplexer, int fd, void* buf, size_t len, IOHandlerContext* ctx);

        virtual int
        onSend(IOPlexer* ioplexer, int fd, const void* buf, size_t len, IOHandlerContext* ctx);

        virtual void
        onFlush(IOPlexer* ioplexer, int fd, IOHandlerContext* ctx);

        bool
        startThread();

private:
        static void
        workerThread(void* ctx);

        virtual void
        onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx);
};

#endif/*__ThreadIOHandler__h__*/

/* vim: set ts=8 sts=8 sw=8 ff=unix et: */
