#ifndef __IOPlexer__h__
#define __IOPlexer__h__

#include <stdio.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>

///////////////////////////////////////////////////////////////////////
//atomic read/write support for 32-bit integer.
#define ATOMIC_INCREMENT(var)                                   \
        ({      uint32_t result = 0;                            \
                asm volatile (                                  \
                        "lock; xaddl %%eax, %0; incl %%eax;"    \
                        : "=m" (var), "=a" (result)             \
                        : "a" (1)                               \
                        : "memory"                              \
                );                                              \
                result;                                         \
        })

#define ATOMIC_DECREMENT(var)                                   \
        ({      uint32_t result = 0;                            \
                asm volatile (                                  \
                        "lock; xaddl %%eax, %0; decl %%eax;"    \
                        : "=m" (var), "=a" (result)             \
                        : "a" (-1)                              \
                        : "memory"                              \
                );                                              \
                result;                                         \
        })

#define ATOMIC_ADD(var, value)                                  \
        ({      uint32_t result = 0;                            \
                asm volatile (                                  \
                        "lock; xaddl %%eax, %0; add %2, %%eax;" \
                        : "=m" (var), "=a" (result)             \
                        : "a" (value)                           \
                        : "memory"                              \
                );                                              \
                result;                                         \
        })

#define ATOMIC_SUB(var, value)  ATOMIC_ADD(var, -value)
#define ATOMIC_INC(var)         ATOMIC_INCREMENT(var)
#define ATOMIC_DEC(var)         ATOMIC_DECREMENT(var)

//atomic read/write support for 64-bit integer.
#define ATOMIC_ADD64(var, value)                                          \
        ({      uint64_t oldval   = 0;                                    \
                uint64_t newval   = 0;                                    \
                uint64_t previous = 0;                                    \
                do {                                                      \
                        oldval = var;                                     \
                        newval = oldval + value;                          \
                        asm volatile (                                    \
                                "push %%ebx;"                             \
                                "push %%ecx;"                             \
                                "movl (%3), %%ebx;"                       \
                                "movl 4(%3), %%ecx;"                      \
                                "lock; cmpxchg8b (%1);"                   \
                                "pop %%ecx;"                              \
                                "pop %%ebx;"                              \
                                : "=A" (previous)                         \
                                : "D" (&var), "0" (oldval), "S" (&newval) \
                                : "memory"                                \
                        );                                                \
                } while(oldval != previous);                              \
                newval;                                                   \
        })

#define ATOMIC_INCREMENT64(var)  ATOMIC_ADD64(var, 1)
#define ATOMIC_DECREMENT64(var)  ATOMIC_ADD64(var, -1)
#define ATOMIC_SUB64(var, value) ATOMIC_ADD64(var, -value)
#define ATOMIC_INC64(var)        ATOMIC_INCREMENT64(var)
#define ATOMIC_DEC64(var)        ATOMIC_DECREMENT64(var)

#define ATOMIC_INT64(var)        ATOMIC_ADD64(var, 0)

///////////////////////////////////////////////////////////////////////
#ifndef ENABLE_STATISTICS
#define ENABLE_STATISTICS       (1)
#endif
///////////////////////////////////////////////////////////////////////
#if ENABLE_STATISTICS

typedef struct 
{
        struct {
                //event pool sizes
                size_t   capacity;
                size_t   count;
                //occurrences of each event
                uint32_t curAcceptEvents;
                uint32_t curConnectEvents;
                uint32_t curReadEvents;
                uint32_t curWriteEvents;
                uint32_t curCloseEvents;
                uint32_t peakAcceptEvents;
                uint32_t peakConnectEvents;
                uint32_t peakReadEvents;
                uint32_t peakWriteEvents;
                uint32_t peakCloseEvents;
                uint64_t totalAcceptEvents;
                uint64_t totalConnectEvents;
                uint64_t totalReadEvents;
                uint64_t totalWriteEvents;
                uint64_t totalCloseEvents;
        } event;

        struct {
                //FD-IOHandler hash table
                uint32_t size;
                uint32_t fdCount;
                uint32_t peakFdCount;
                uint64_t totalFdCount;
                uint32_t maxCollision;
        } hash;

        struct {
                uint32_t running;
                uint32_t minCount;
                uint32_t maxCount;
                uint32_t count;
                uint32_t peakCount;
                uint64_t totalCount;
                uint32_t activeCount;
                uint32_t peakActiveCount;
                uint32_t totalActiveCount;
        } thread;

        struct {
                uint32_t size;
                uint32_t peakSize;
                uint64_t totalSize;
        } taskqueue;

        struct {
                uint32_t hashSize;
                uint32_t slotCount;
                uint32_t pendingSlotCount;
                uint32_t peakSlotCount;
                uint64_t totalSlotCount;

                struct {
                        uint32_t size;
                        uint32_t peakSize;
                        uint64_t totalSize;
                        uint32_t chunkCount;
                        uint32_t peakChunkCount;
                        uint64_t totalChunkCount;
                }
                incoming, outgoing;
        } buffer;

} IOStatistics;

#endif
///////////////////////////////////////////////////////////////////////
class IOPlexer;
class IOHandlerContext;

class IOHandler
{       friend class IOPlexer;
public:
        enum {
                EVENT_CONNECT   = 0x01,
                EVENT_ACCEPT    = 0x02,
                EVENT_READ      = 0x04,
                EVENT_WRITE     = 0x08,
                EVENT_CLOSE     = 0x10,
                EVENT_READWRITE = EVENT_READ | EVENT_WRITE,
                EVENT_DEFAULT   = EVENT_READ | EVENT_WRITE,
                EVENT_ALL       = EVENT_CONNECT| EVENT_ACCEPT | EVENT_READ | EVENT_WRITE | EVENT_CLOSE
        };

        typedef void (*HandleRoutine )(IOPlexer* ioplexer, int fd, void* userData, int events);
        typedef void (*CleanupRoutine)(void* userData);

        IOHandler(
                HandleRoutine  handleRoutine  = NULL,
                CleanupRoutine cleanupRoutine = NULL,
                void*          userData       = NULL
        );

        virtual void
        shutdown();

        unsigned int
        addRef();

        unsigned int
        release();

protected:
        virtual void
        onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx);

#if ENABLE_STATISTICS
        virtual void
        getStatistics(IOStatistics* statistics);
#endif

        virtual
        ~IOHandler();

private:
        unsigned int   refcount;
        HandleRoutine  handleRoutine;
        CleanupRoutine cleanupRoutine;
        void*          userData;
};

///////////////////////////////////////////////////////////////////////
class IOHandlerContext
{
public:
        IOHandlerContext(void* ctx = NULL);

        unsigned int
        addRef();

        unsigned int
        release();

        void*
        getContext() const;

        void
        setContext(void* ctx);

protected:
        virtual
        ~IOHandlerContext();

private:
        unsigned int refcount;
        void*        ctx;
};

///////////////////////////////////////////////////////////////////////

class IOPlexer
{
        enum {
                DEFAULT_EVENT_CAPACITY  = 32768,
                DEFAULT_HASH_SIZE       = 32768,
                INIT_EVENT_COUNT        = 4096
        };

        struct IOHandlerSlot
        {
                IOHandlerSlot*    next;
                int               fd;
                int               eventMask;
                IOHandler*        handler;
                IOHandlerContext* ctx;
                uint64_t          touched;

                IOHandlerSlot(int fd = -1);

                ~IOHandlerSlot();

                IOHandler*
                setHandler(IOHandler* handler);

                IOHandlerContext*
                setContext(IOHandlerContext* ctx);

                void
                closeFd();
        };

        pthread_mutex_t mutex;
        int             handle;
        IOHandlerSlot** handlerMap;
        size_t          hashSize;
        size_t          fdCount;
        size_t          peakFdCount;
        size_t          totalFdCount;
        void*           events;
        size_t          eventCapacity;
        size_t          eventCount;
        size_t          lastReceived;

        struct IORenewRequest
        {
                IORenewRequest* prev;
                IORenewRequest* next;
                int             fd;

                IORenewRequest(int fd = -1);
                ~IORenewRequest();
        };

        IORenewRequest* requestHead;
        IORenewRequest* requestTail;

#if ENABLE_STATISTICS
        uint32_t        curAcceptEvents;
        uint32_t        curConnectEvents;
        uint32_t        curReadEvents;
        uint32_t        curWriteEvents;
        uint32_t        curCloseEvents;
        uint32_t        peakAcceptEvents;
        uint32_t        peakConnectEvents;
        uint32_t        peakReadEvents;
        uint32_t        peakWriteEvents;
        uint32_t        peakCloseEvents;
        uint64_t        totalAcceptEvents;
        uint64_t        totalConnectEvents;
        uint64_t        totalReadEvents;
        uint64_t        totalWriteEvents;
        uint64_t        totalCloseEvents;
#endif

public:
        IOPlexer(int capacity = -1, int hashSize = -1);

        ~IOPlexer();

        void
        clear();

        bool
        set(
                int                       fd,
                int                       eventMask,
                IOHandler*                handler,
                IOHandlerContext*         ctx            = NULL
        );

        bool
        set(
                int                       fd,
                int                       eventMask,
                IOHandler::HandleRoutine  handleRoutine,
                IOHandler::CleanupRoutine cleanupRoutine = NULL,
                void*                     userData       = NULL
        );

        bool
        unset(int fd);

        bool
        reenable(int fd);

        bool
        raise(int fd, int events);

        bool
        dispatch(int idleTimeoutMillis = -1);

        void
        loop(int idleTimeoutMillis = -1);

#if ENABLE_STATISTICS
        void
        getStatistics(IOStatistics* statistics);
#endif
private:
        size_t 
        hash(int fd);
};

#endif/*__IOPlexer__h__*/

/* vim: set ts=8 sts=8 sw=8 ff=unix et: */
