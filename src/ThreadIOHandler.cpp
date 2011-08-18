#include "ThreadIOHandler.h"
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <malloc.h>
#include <pthread.h>
#include <syslog.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>

extern "C" void
__IOPlexerDebugOutput(
        const char* file,
        int         line,
        const char* function,
        const char* fmt, ...);

#define DBG(msg...)     __IOPlexerDebugOutput(__FILE__, __LINE__, __FUNCTION__, ##msg)

///////////////////////////////////////////////////////////////////////
ThreadIOHandler::Chunk::Chunk(const char* buf, size_t size)
{
        this->prev = NULL;
        this->next = NULL;
        this->buf  = NULL;
        this->size = 0;

        if(buf) {
                if(size == size_t(-1)) {
                        size = strlen(buf) + 1;
                }

                this->buf = (char*) malloc(size);

                if(this->buf) {
                        memcpy(this->buf, buf, size);
                        this->size = size;
                }
        }
}

ThreadIOHandler::Chunk::~Chunk()
{
        if(buf) {
                free(buf);
        }
}

///////////////////////////////////////////////////////////////////////
ThreadIOHandler::Buffer::Buffer(
#if ENABLE_STATISTICS
        uint32_t &gCount,
        uint32_t &gPeakCount,
        uint64_t &gTotalCount,
        uint32_t &gCapacity,
        uint32_t &gPeakCapacity,
        uint64_t &gTotalCapacity
#endif
)
#if ENABLE_STATISTICS
: gCount        (gCount),
  gPeakCount    (gPeakCount),
  gTotalCount   (gTotalCount),
  gCapacity     (gCapacity),
  gPeakCapacity (gPeakCapacity),
  gTotalCapacity(gTotalCapacity)
#endif
{
        pthread_mutex_init(&this->mutex       , NULL);
        pthread_cond_init (&this->readEvent   , NULL);
        pthread_cond_init (&this->writtenEvent, NULL);

        this->head     = NULL;
        this->tail     = NULL;
        this->cur      = NULL;
        this->fense    = 0;
        this->offset   = 0;
        this->count    = 0;
        this->capacity = 0;
}

ThreadIOHandler::Buffer::~Buffer()
{
        clear();
        pthread_cond_broadcast(&writtenEvent);
        pthread_cond_broadcast(&readEvent);
        pthread_cond_destroy  (&writtenEvent);
        pthread_cond_destroy  (&readEvent);
        pthread_mutex_destroy (&mutex);
}

void
ThreadIOHandler::Buffer::clear()
{
        pthread_mutex_lock(&mutex);

        register Chunk* p = head;

        while(p) {
                register Chunk* next = p->next;

                #if ENABLE_STATISTICS
                ATOMIC_DEC(gCount);
                ATOMIC_SUB(gCapacity, p->size);
                #endif

                delete p;
                p = next;
        }
        head     = NULL;
        tail     = NULL;
        cur      = NULL;
        fense    = 0;
        offset   = 0;
        count    = 0;
        capacity = 0;

        pthread_mutex_unlock(&mutex);
}

bool
ThreadIOHandler::Buffer::append(const char* buf, size_t size)
{
        Chunk* chunk = new Chunk(buf, size);

        if(chunk == NULL) {
                //insufficient memory
                return false;
        }

        pthread_mutex_lock(&mutex);

        if(tail) {
                chunk->prev = tail;
                tail = tail->next = chunk;
        } else {
                head = tail = chunk;
        }
        count++;
        capacity += chunk->size;

#if ENABLE_STATISTICS
        ATOMIC_INC(gCount);
        ATOMIC_ADD(gCapacity, chunk->size);
        ATOMIC_INC(gTotalCount);
        ATOMIC_ADD(gTotalCapacity, chunk->size);

        if(gCount > gPeakCount) {
                gPeakCount = gCount;
        }
        if(gCapacity > gPeakCapacity) {
                gPeakCapacity = gCapacity;
        }
#endif
        pthread_cond_signal(&writtenEvent);
        pthread_mutex_unlock(&mutex);
        return true;
}

size_t
ThreadIOHandler::Buffer::fetch(char* buf, size_t size, bool perChunk)
{
        size_t bytes = 0;

        pthread_mutex_lock(&mutex);

        if(head) {
                if(cur == NULL) {
                        cur    = head;
                        offset = fense;
                }

                while(size > 0) {
                        size_t len = cur->size - offset;

                        if(cur == tail && len == 0) {
                                break;
                        }

                        if(len > size) {
                                len = size;
                        }

                        if(len > 0) {
                                memcpy(buf + bytes, cur->buf + offset, len);
                                bytes  += len;
                                offset += len;
                                size   -= len;
                        }

                        if(cur != tail && offset == cur->size) {
                                cur    = cur->next;
                                offset = 0;
                        }

                        if(perChunk && len > 0) {
                                break;
                        }
                }
        }
        pthread_mutex_unlock(&mutex);
        return bytes;
}

void
ThreadIOHandler::Buffer::rewind()
{
        pthread_mutex_lock(&mutex);
        cur    = head;
        offset = fense;
        pthread_mutex_unlock(&mutex);
}

void
ThreadIOHandler::Buffer::unwind(size_t size)
{
        pthread_mutex_lock(&mutex);

        while(cur && size > 0) {

                if(offset > size) {
                        offset -= size;

                        if(cur == head && offset < fense) {
                                offset = fense;
                        }
                        break;
                }

                size -= offset;

                if(cur == head) {
                        offset = fense;
                        break;
                }

                cur    = cur->prev;
                offset = cur->size;
        }
        pthread_mutex_unlock(&mutex);
}

void
ThreadIOHandler::Buffer::consume(size_t size)
{
        bool changed = false;

        pthread_mutex_lock(&mutex);

        for(cur = head; cur && size > 0; ) {

                if(fense + size < cur->size) {
                        fense  += size;
                        offset  = fense;
                        changed = true;
                        break;
                }

                if(cur->prev) {
                        cur->prev->next = cur->next;
                } else {
                        head = cur->next;
                }

                if(cur->next) {
                        cur->next->prev = cur->prev;
                } else {
                        tail = cur->prev;
                }

                size     -= cur->size;
                capacity -= cur->size;
                count--;

                #if ENABLE_STATISTICS
                ATOMIC_DEC(gCount);
                ATOMIC_SUB(gCapacity, cur->size);
                #endif

                delete cur;
                cur     = head;
                fense   = 0;
                offset  = 0;
                changed = true;
        }

        if(changed) {
                pthread_cond_signal(&readEvent);
        }
        pthread_mutex_unlock(&mutex);
}

bool
ThreadIOHandler::Buffer::isEmpty()
{
        pthread_mutex_lock(&mutex);
        bool empty = (tail == NULL) || (cur == tail && offset == cur->size);
        pthread_mutex_unlock(&mutex);
        return empty;
}

size_t
ThreadIOHandler::Buffer::getCapacity() const
{
        return capacity;
}

size_t
ThreadIOHandler::Buffer::getCount() const
{
        return count;
}

bool
ThreadIOHandler::Buffer::checkReadable(int milliseconds)
{
        bool empty = false;

        pthread_mutex_lock(&mutex);

        empty = (tail == NULL) || (cur == tail && offset == cur->size);

        if(empty) {
                if(milliseconds < 0) {
                        pthread_cond_wait(&writtenEvent, &mutex);
                } else {
                        struct timeval  tv;
                        struct timespec timeout;
                        gettimeofday(&tv, NULL);

                        tv.tv_sec  = (tv.tv_sec  + (milliseconds / 1000)) +
                                     (tv.tv_usec + (milliseconds % 1000) * 1000) / 1000000;
                        tv.tv_usec = (tv.tv_usec + (milliseconds % 1000) * 1000) % 1000000;

                        timeout.tv_sec  = tv.tv_sec;
                        timeout.tv_nsec = tv.tv_usec * 1000;
                        pthread_cond_timedwait(&writtenEvent, &mutex, &timeout);
                }
                empty = (tail == NULL) || (cur == tail && offset == cur->size);
        }
        pthread_mutex_unlock(&mutex);

        return !empty;
}

bool
ThreadIOHandler::Buffer::checkWritable(size_t maxCapacity, int milliseconds)
{
        bool full = false;

        pthread_mutex_lock(&mutex);

        full = (capacity >= maxCapacity);

        if(full) {
                if(milliseconds < 0) {
                        pthread_cond_wait(&readEvent, &mutex);
                } else {
                        struct timeval  tv;
                        struct timespec timeout;
                        gettimeofday(&tv, NULL);

                        tv.tv_sec  = (tv.tv_sec  + (milliseconds / 1000)) +
                                     (tv.tv_usec + (milliseconds % 1000) * 1000) / 1000000;
                        tv.tv_usec = (tv.tv_usec + (milliseconds % 1000) * 1000) % 1000000;

                        timeout.tv_sec  = tv.tv_sec;
                        timeout.tv_nsec = tv.tv_usec * 1000;
                        pthread_cond_timedwait(&writtenEvent, &mutex, &timeout);
                }
                full = (capacity >= maxCapacity);
        }
        pthread_mutex_unlock(&mutex);

        return !full;
}

///////////////////////////////////////////////////////////////////////

ThreadIOHandler::EventSlot::EventSlot(IOPlexer* ioplexer, int events, IOHandlerContext* ctx)
{
        this->prev     = NULL;
        this->next     = NULL;
        this->ioplexer = ioplexer;
        this->events   = events;
        this->ctx      = ctx;

        if(this->ctx) {
                this->ctx->addRef();
        }
}

ThreadIOHandler::EventSlot::~EventSlot()
{
        if(this->ctx) {
                this->ctx->release();
        }
}

///////////////////////////////////////////////////////////////////////

ThreadIOHandler::BufferSlot::BufferSlot(
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
)
#if ENABLE_STATISTICS
: incoming(incoming_chunkCount,
           incoming_peakChunkCount,
           incoming_totalChunkCount,
           incoming_size,
           incoming_peakSize,
           incoming_totalSize),
  outgoing(outgoing_chunkCount,
           outgoing_peakChunkCount,
           outgoing_totalChunkCount,
           outgoing_size,
           outgoing_peakSize,
           outgoing_totalSize),
  pendingCount(pendingCount)
#endif
{
        pthread_mutex_init(&mutex        , NULL);
        pthread_cond_init (&releasedEvent, NULL);

        next       = 0;
        fd         = -1;
        closed     = false;
        refcount   = 1;
        occupied   = false;
        reentrance = 0;
        occurrence = 0;
        eventHead  = NULL;
        eventTail  = NULL;

#if ENABLE_STATISTICS
        ATOMIC_INCREMENT(pendingCount);
#endif
}

ThreadIOHandler::BufferSlot::~BufferSlot()
{
        clearPendingEvents();

        pthread_mutex_lock(&mutex);
        reentrance = 0;
        pthread_cond_broadcast(&releasedEvent);
        pthread_mutex_unlock(&mutex);

        pthread_cond_destroy(&releasedEvent);
        pthread_mutex_destroy(&mutex);

#if ENABLE_STATISTICS
        ATOMIC_DECREMENT(pendingCount);
#endif
}

unsigned int
ThreadIOHandler::BufferSlot::addRef()
{
        return ATOMIC_INCREMENT(refcount);
}

unsigned int
ThreadIOHandler::BufferSlot::release()
{
        unsigned int result = ATOMIC_DECREMENT(refcount);

        if(result == 0) {
                delete this;
                return 0;
        }
        return result;
}

void
ThreadIOHandler::BufferSlot::lock()
{
        tryLock(-1);
}

bool
ThreadIOHandler::BufferSlot::tryLock(int milliseconds)
{
        bool result = false;

        pthread_mutex_lock(&mutex);

        while(occupied && occupied != pthread_self()) {
                if(milliseconds == 0 || reentrance == 0) {
                        goto leave;
                }
                if(milliseconds < 0) {
                        pthread_cond_wait(&releasedEvent, &mutex);
                        continue;
                }

                struct timeval  tv;
                struct timespec timeout;
                gettimeofday(&tv, NULL);

                tv.tv_sec  = (tv.tv_sec  + (milliseconds / 1000)) +
                             (tv.tv_usec + (milliseconds % 1000) * 1000) / 1000000;
                tv.tv_usec = (tv.tv_usec + (milliseconds % 1000) * 1000) % 1000000;

                timeout.tv_sec  = tv.tv_sec;
                timeout.tv_nsec = tv.tv_usec * 1000;

                if(pthread_cond_timedwait(&releasedEvent, &mutex, &timeout) != 0) {
                        goto leave;
                }
        }

        if(++reentrance == 1) {
                occupied = pthread_self();
        }
        result = true;
leave:
        pthread_mutex_unlock(&mutex);
        return result;
}

void
ThreadIOHandler::BufferSlot::unlock()
{
        pthread_mutex_lock(&mutex);

        if(occupied == pthread_self()) {
                if(--reentrance == 0) {
                        occupied = 0;
                        pthread_cond_signal(&releasedEvent);
                }
        }
        pthread_mutex_unlock(&mutex);
}

unsigned int
ThreadIOHandler::BufferSlot::addPendingEvent(
        IOPlexer*         ioplexer,
        int               events,
        IOHandlerContext* ctx
)
{
        pthread_mutex_lock(&mutex);

        unsigned int n = ATOMIC_INCREMENT(occurrence);

        if(n > 1) {
                EventSlot* e = new EventSlot(ioplexer, events, ctx);

                if(eventTail) {
                        e->prev = eventTail;
                        eventTail = eventTail->next = e;
                } else {
                        eventHead = eventTail = e;
                }
        }
        pthread_mutex_unlock(&mutex);
        return n;
}

unsigned int
ThreadIOHandler::BufferSlot::getPendingEvent(
        IOPlexer*         &ioplexer,
        int               &events,
        IOHandlerContext* &ctx
)
{
        pthread_mutex_lock(&mutex);

        unsigned int n = ATOMIC_DECREMENT(occurrence);

        if(eventHead) {
                EventSlot* e = eventHead;
                
                if(e->prev) {
                        e->prev->next = e->next;
                } else {
                        eventHead = e->next;
                }
                if(e->next) {
                        e->next->prev = e->prev;
                } else {
                        eventTail = e->prev;
                }

                ioplexer = e->ioplexer;
                events   = e->events;
                ctx      = e->ctx;

                e->ioplexer = NULL;
                e->events   = 0;
                e->ctx      = NULL;
                delete e;
        } else {
                ioplexer = NULL;
                events   = 0;
                ctx      = NULL;
        }
        pthread_mutex_unlock(&mutex);
        return n;
}

void
ThreadIOHandler::BufferSlot::clearPendingEvents()
{
        pthread_mutex_lock(&mutex);

        register EventSlot* e = eventHead;
        while(e) {
                register EventSlot* next = e->next;
                delete e;
                e = next;
                ATOMIC_DECREMENT(occurrence);
        }
        eventHead = NULL;
        eventTail = NULL;

        pthread_mutex_unlock(&mutex);
}
///////////////////////////////////////////////////////////////////////
ThreadIOHandler::BufferMap::BufferMap(int hashSize)
{
        pthread_mutex_init(&mutex, NULL);

        if(hashSize <= 0) {
                hashSize = DEFAULT_HASH_SIZE;
        }

        this->hashSize                 = hashSize;
        this->hashTable                = (BufferSlot**) calloc(hashSize, sizeof(BufferSlot*));
        this->count                    = 0;
#if ENABLE_STATISTICS
        this->pendingCount             = 0;
        this->peakCount                = 0;
        this->totalCount               = 0;
        this->incoming_size            = 0;
        this->incoming_peakSize        = 0;
        this->incoming_totalSize       = 0;
        this->incoming_chunkCount      = 0;
        this->incoming_peakChunkCount  = 0;
        this->incoming_totalChunkCount = 0;
        this->outgoing_size            = 0;
        this->outgoing_peakSize        = 0;
        this->outgoing_totalSize       = 0;
        this->outgoing_chunkCount      = 0;
        this->outgoing_peakChunkCount  = 0;
        this->outgoing_totalChunkCount = 0;
#endif
}

ThreadIOHandler::BufferMap::~BufferMap()
{
        clear();

        if(hashTable) {
                free(hashTable);
                hashTable = NULL;
        }
        pthread_mutex_destroy(&mutex);
}

size_t
ThreadIOHandler::BufferMap::getHashSize() const
{
        return hashSize;
}

size_t
ThreadIOHandler::BufferMap::getCount() const
{
        return count;
}

void
ThreadIOHandler::BufferMap::clear()
{
        pthread_mutex_lock(&mutex);
        if(hashTable) {
                for(size_t i = 0; i < hashSize; i++) {
                        register BufferSlot* slot = hashTable[i];

                        while(slot) {
                                hashTable[i] = slot->next;
                                count--;
                                pthread_mutex_unlock(&mutex);

                                slot->release();

                                pthread_mutex_lock(&mutex);
                                slot = hashTable[i];
                        }
                }
        }
        pthread_mutex_unlock(&mutex);
}

ThreadIOHandler::BufferSlot*
ThreadIOHandler::BufferMap::get(int fd)
{
        BufferSlot* slot = NULL;
        pthread_mutex_lock(&mutex);

        if(hashTable) {
                slot = hashTable[hash(fd)];

                for(; slot; slot = slot->next) {
                        if(slot->fd == fd) {
                                slot->addRef();
                                break;
                        }
                }
        }
        pthread_mutex_unlock(&mutex);
        return slot;
}

ThreadIOHandler::BufferSlot*
ThreadIOHandler::BufferMap::acquire(int fd)
{
        BufferSlot* slot = NULL;
        pthread_mutex_lock(&mutex);

        if(hashTable) {
                size_t ndx = hash(fd);
                slot = hashTable[ndx];

                for(; slot; slot = slot->next) {
                        if(slot->fd == fd) {
                                slot->addRef();
                                break;
                        }
                }

                if(slot == NULL) {
                        slot = new BufferSlot(
                                #if ENABLE_STATISTICS
                                pendingCount,
                                incoming_size,
                                incoming_peakSize,
                                incoming_totalSize,
                                incoming_chunkCount,
                                incoming_peakChunkCount,
                                incoming_totalChunkCount,
                                outgoing_size,
                                outgoing_peakSize,
                                outgoing_totalSize,
                                outgoing_chunkCount,
                                outgoing_peakChunkCount,
                                outgoing_totalChunkCount
                                #endif
                        );

                        if(slot) {
                                slot->fd       = fd;
                                slot->next     = hashTable[ndx];
                                hashTable[ndx] = slot;
                                count++;
                                #if ENABLE_STATISTICS
                                totalCount++;

                                if(count > peakCount) {
                                        peakCount = count;
                                }
                                #endif
                                slot->addRef();
                        }
                }
        }
        pthread_mutex_unlock(&mutex);
        return slot;
}

void
ThreadIOHandler::BufferMap::remove(int fd)
{
        BufferSlot* slot = NULL;

        pthread_mutex_lock(&mutex);

        if(hashTable) {
                BufferSlot* prev = NULL;
                size_t      ndx  = hash(fd);

                for(slot = hashTable[ndx]; slot; prev = slot, slot = slot->next) {
                        if(slot->fd == fd) {
                                if(prev) {
                                        prev->next = slot->next;
                                } else {
                                        hashTable[ndx] = slot->next;
                                }
                                count--;
                                break;
                        }
                }
        }
        pthread_mutex_unlock(&mutex);

        if(slot) {
                slot->closed = true;
                slot->release();
        }
}

size_t 
ThreadIOHandler::BufferMap::hash(int fd)
{
        return size_t(fd % hashSize);
}

#if ENABLE_STATISTICS
void
ThreadIOHandler::BufferMap::getStatistics(IOStatistics* s)
{
        pthread_mutex_lock(&mutex);
        s->buffer.hashSize                 = hashSize;
        s->buffer.slotCount                = count;
        s->buffer.pendingSlotCount         = pendingCount;
        s->buffer.peakSlotCount            = peakCount;
        s->buffer.totalSlotCount           = totalCount;
        s->buffer.incoming.size            = incoming_size;
        s->buffer.incoming.peakSize        = incoming_peakSize;
        s->buffer.incoming.totalSize       = incoming_totalSize;
        s->buffer.incoming.chunkCount      = incoming_chunkCount;
        s->buffer.incoming.peakChunkCount  = incoming_peakChunkCount;
        s->buffer.incoming.totalChunkCount = incoming_totalChunkCount;
        s->buffer.outgoing.size            = outgoing_size;
        s->buffer.outgoing.peakSize        = outgoing_peakSize;
        s->buffer.outgoing.totalSize       = outgoing_totalSize;
        s->buffer.outgoing.chunkCount      = outgoing_chunkCount;
        s->buffer.outgoing.peakChunkCount  = outgoing_peakChunkCount;
        s->buffer.outgoing.totalChunkCount = outgoing_totalChunkCount;
        pthread_mutex_unlock(&mutex);
}
#endif

///////////////////////////////////////////////////////////////////////
ThreadIOHandler::Task::Task(
        IOPlexer*         ioplexer,
        int               fd,
        int               events,
        IOHandlerContext* ctx,
        BufferSlot*       slot
)
{
        this->prev     = NULL;
        this->next     = NULL;
        this->ioplexer = ioplexer;
        this->fd       = fd;
        this->events   = events;
        this->ctx      = ctx;
        this->slot     = slot;

        if(this->ctx) {
                this->ctx->addRef();
        }
        if(this->slot) {
                this->slot->addRef();
        }
}

ThreadIOHandler::Task::~Task()
{
        if(ctx) {
                ctx->release();
        }
        if(slot) {
                slot->release();
        }
}

///////////////////////////////////////////////////////////////////////

ThreadIOHandler::TaskQueue::TaskQueue()
{
        pthread_mutex_init(&mutex, NULL);
        pthread_cond_init (&event, NULL);
        head    = NULL;
        tail    = NULL;
        size    = 0;
        running = true;
#if ENABLE_STATISTICS
        peak    = 0;
        total   = 0;
#endif
}

ThreadIOHandler::TaskQueue::~TaskQueue()
{
        preNotifyStop();
        clear();
        pthread_cond_destroy (&event);
        pthread_mutex_destroy(&mutex);
}

size_t
ThreadIOHandler::TaskQueue::getSize() const
{
        return size;
}

bool
ThreadIOHandler::TaskQueue::isRunning() const
{
        return running;
}

void
ThreadIOHandler::TaskQueue::preNotifyStop()
{
        running = false;
}

void
ThreadIOHandler::TaskQueue::clear()
{
        pthread_mutex_lock(&mutex); 

        register Task* task = head;

        while(task) {
                if(task->prev) {
                        task->prev->next = task->next;
                } else {
                        head = task->next;
                }
                if(task->next) {
                        task->next->prev = task->prev;
                } else {
                        tail = task->prev;
                }
                size--;
                pthread_mutex_unlock(&mutex); 

                delete task;

                pthread_mutex_lock(&mutex); 
                task = head;
        }

        pthread_cond_broadcast(&event);
        pthread_mutex_unlock(&mutex); 
}

bool
ThreadIOHandler::TaskQueue::enqueue(
        IOPlexer*         ioplexer,
        int               fd,
        int               events,
        IOHandlerContext* ctx,
        BufferSlot*       slot
)
{
        bool result = false;

        if(slot) {
                pthread_mutex_lock(&mutex); 

                if(slot->addPendingEvent(ioplexer, events, ctx) == 1) {
                        Task* task = new Task(ioplexer, fd, events, ctx, slot);

                        if(task) {
                                if(tail) {
                                        task->prev = tail;
                                        tail = tail->next = task;
                                } else {
                                        head = tail = task;
                                }
                                size++;

                                #if ENABLE_STATISTICS
                                total++;
                                if(size > peak) {
                                        peak = size;
                                }
                                #endif

                                result = true;
                                pthread_cond_signal(&event);
                        }
                }
                pthread_mutex_unlock(&mutex); 
        }
        return result;
}

bool
ThreadIOHandler::TaskQueue::dequeue(
        IOPlexer*         &ioplexer,
        int               &fd,
        int               &events,
        IOHandlerContext* &ctx,
        BufferSlot*       &slot,
        int               milliseconds
)
{
        bool result = false;
        int  error  = 0;
        pthread_mutex_lock(&mutex); 

        while(running && head == NULL) {
                if(milliseconds < 0) {
                        error = pthread_cond_wait(&event, &mutex);
                } else {
                        struct timeval  tv;
                        struct timespec timeout;
                        gettimeofday(&tv, NULL);

                        tv.tv_sec  = (tv.tv_sec  + (milliseconds / 1000)) +
                                     (tv.tv_usec + (milliseconds % 1000) * 1000) / 1000000;
                        tv.tv_usec = (tv.tv_usec + (milliseconds % 1000) * 1000) % 1000000;

                        timeout.tv_sec  = tv.tv_sec;
                        timeout.tv_nsec = tv.tv_usec * 1000;
                        error = pthread_cond_timedwait(&event, &mutex, &timeout);

                        if(error == ETIMEDOUT) {
                                DBG("@@fd[-1]: workerThread[%08x] - pthread_cond_timedwait returned error ETIMEOUT.", (int) pthread_self());
                                break;
                        }
                }

                if(error == 0) {
                        continue;
                }

                switch(error) {
                        case ETIMEDOUT:
                        {       DBG("@@fd[-1]: workerThread[%08x] - pthread_cond_%swait returned error ETIMEOUT.", (int) pthread_self(), ((milliseconds < 0)? "": "timed"));
                                break;
                        }
                        case EINVAL:
                        {       DBG("@@fd[-1]: workerThread[%08x] - pthread_cond_%swait returned error EINVAL.", (int) pthread_self(), ((milliseconds < 0)? "": "timed"));
                                break;
                        }
                        case EPERM:
                        {       DBG("@@fd[-1]: workerThread[%08x] - pthread_cond_%swait returned error EPERM", (int) pthread_self(), ((milliseconds < 0)? "": "timed"));
                                break;
                        }
                        case EINTR:
                        {       DBG("@@fd[-1]: workerThread[%08x] - pthread_cond_%swait returned error EINTR", (int) pthread_self(), ((milliseconds < 0)? "": "timed"));
                                break;
                        }
                        default:
                        {       DBG("@@fd[-1]: workerThread[%08x] - pthread_cond_%swait returned error %d.", (int) pthread_self(), ((milliseconds < 0)? "": "timed"), error);
                                break;
                        }
                }
        }

        if(head) {
                Task* task = head;

                if(task->prev) {
                        task->prev->next = task->next;
                } else {
                        head = task->next;
                }

                if(task->next) {
                        task->next->prev = task->prev;
                } else {
                        tail = task->prev;
                }
                size--;

                ioplexer = task->ioplexer;
                fd       = task->fd;
                events   = task->events;
                ctx      = task->ctx;
                slot     = task->slot;

                task->ioplexer = NULL;
                task->ctx      = NULL;
                task->slot     = NULL;
                delete task;

                result = true;
        } else {
                DBG("@@fd[-1]: workerThread[%08x] - no task in queue.", (int) pthread_self());
        }
        pthread_mutex_unlock(&mutex); 
        return result;
}

bool
ThreadIOHandler::TaskQueue::reschedule(BufferSlot* slot)
{
        bool result = false;

        if(slot) {
                IOPlexer*         ioplexer = NULL;
                int               events   = 0;
                IOHandlerContext* ctx      = NULL;

                pthread_mutex_lock(&mutex); 

                if(slot->getPendingEvent(ioplexer, events, ctx) > 0) {
                        Task* task = new Task(ioplexer, slot->fd, events, ctx, slot);

                        if(task) {
                                if(tail) {
                                        task->prev = tail;
                                        tail = tail->next = task;
                                } else {
                                        head = tail = task;
                                }
                                size++;

                                #if ENABLE_STATISTICS
                                total++;
                                if(size > peak) {
                                        peak = size;
                                }
                                #endif

                                result = true;
                                pthread_cond_signal(&event);
                        }

                        if(ctx) {
                                ctx->release();
                        }
                }
                pthread_mutex_unlock(&mutex); 
        }
        return result;
}

#if ENABLE_STATISTICS
void
ThreadIOHandler::TaskQueue::getStatistics(IOStatistics* s)
{
        pthread_mutex_lock(&mutex); 
        s->taskqueue.size      = size;
        s->taskqueue.peakSize  = peak;
        s->taskqueue.totalSize = total;
        pthread_mutex_unlock(&mutex); 
}
#endif

///////////////////////////////////////////////////////////////////////

ThreadIOHandler::Thread::Thread(ThreadRoutine routine, void* ctx)
{
        this->prev     = NULL;
        this->next     = NULL;
        this->id       = 0;
        this->routine  = routine;
        this->ctx      = ctx;
        this->refcount = 1;
}

ThreadIOHandler::Thread::~Thread()
{
}

unsigned int
ThreadIOHandler::Thread::addRef()
{
        return ATOMIC_INCREMENT(refcount);
}

unsigned int
ThreadIOHandler::Thread::release()
{
        unsigned int result = ATOMIC_DECREMENT(refcount);
        if(result == 0) {
                delete this;
                return 0;
        }
        return result;
}

///////////////////////////////////////////////////////////////////////
ThreadIOHandler::ThreadContext::ThreadContext(
        ThreadIOHandler* handler,
        Thread*          thread
)
{
        this->handler  = handler;
        this->thread   = thread;
        this->refcount = 1;

        if(this->handler) {
                this->handler->addRef();
        }
        if(this->thread) {
                this->thread->addRef();
        }
}

ThreadIOHandler::ThreadContext::~ThreadContext()
{
        if(thread) {
                thread->release();
        }
        if(handler) {
                handler->release();
        }
}

unsigned int
ThreadIOHandler::ThreadContext::addRef()
{
        return ATOMIC_INCREMENT(this->refcount);
}

unsigned int
ThreadIOHandler::ThreadContext::release()
{
        unsigned int result = ATOMIC_DECREMENT(refcount);
        if(result == 0) {
                delete this;
                return 0;
        }
        return result;
}

///////////////////////////////////////////////////////////////////////

ThreadIOHandler::ThreadPool::ThreadPool()
{
        pthread_mutex_init(&mutex, NULL);

        head    = NULL;
        tail    = NULL;
        running = true;
        count   = 0;

        #if ENABLE_STATISTICS
        peak    = 0;
        total   = 0;
        #endif
}

ThreadIOHandler::ThreadPool::~ThreadPool()
{
        preNotifyStop();
        joinAllThreads();
        pthread_mutex_destroy(&mutex);
}

size_t
ThreadIOHandler::ThreadPool::getCount() const
{
        return count;
}

bool
ThreadIOHandler::ThreadPool::isRunning() const
{
        return running;
}

void
ThreadIOHandler::ThreadPool::addToList(Thread* thread)
{
        //NOTE: should be run within the lock

        if(tail) {
                thread->prev = tail;
                tail = tail->next = thread;
        } else {
                head = tail = thread;
        }
        count++;

        #if ENABLE_STATISTICS
        total++;

        if(count > peak) {
                peak = count;
        }
        #endif
}

ThreadIOHandler::Thread*
ThreadIOHandler::ThreadPool::removeFromList(Thread* thread)
{
        //NOTE: should be run within the lock

        if(thread->prev) {
                thread->prev->next = thread->next;
        } else {
                head = thread->next;
        }
        if(thread->next) {
                thread->next->prev = thread->prev;
        } else {
                tail = thread->prev;
        }
        count--;
        return thread;
}

void
ThreadIOHandler::ThreadPool::preNotifyStop()
{
        running = false;
}

void
ThreadIOHandler::ThreadPool::joinAllThreads()
{
        pthread_mutex_lock(&mutex);
        while(head) {
                register Thread* thread = removeFromList(head);
                pthread_mutex_unlock(&mutex);

                if(thread->id) {
                        pthread_join(thread->id, NULL);
                        thread->id = 0;
                }
                thread->release();

                pthread_mutex_lock(&mutex);
        }
        pthread_mutex_unlock(&mutex);
}

bool
ThreadIOHandler::ThreadPool::startThread(
        ThreadIOHandler* handler,
        ThreadRoutine    routine,
        void*            context
)
{
        if(!running) {
                return false;
        }

        Thread* thread = new Thread(routine, context);

        if(thread) {
                ThreadContext* ctx = new ThreadContext(handler, thread);

                if(ctx) {
                        if(pthread_create(&thread->id, NULL, threadRoutine, ctx) == 0) {
                                pthread_mutex_lock(&mutex);
                                addToList(thread);
                                pthread_mutex_unlock(&mutex);
                                return true;
                        }
                        ctx->release();
                }
                thread->release();
        }
        return false;
}

void*
ThreadIOHandler::ThreadPool::threadRoutine(void* context)
{
        sigset_t signals;
        sigemptyset(&signals);
        sigaddset(&signals, SIGHUP);
        sigaddset(&signals, SIGINT);
        sigaddset(&signals, SIGFPE);
        sigaddset(&signals, SIGPIPE);
        sigaddset(&signals, SIGUSR1);
        sigaddset(&signals, SIGUSR2);
        pthread_sigmask(SIG_BLOCK, &signals, NULL);

        ThreadContext* ctx = static_cast<ThreadContext*>(context);

        if(ctx) {
                ctx->thread->routine(ctx->thread->ctx);
                ctx->release();
        }
        pthread_exit(NULL);
        return NULL;
}

#if ENABLE_STATISTICS
void
ThreadIOHandler::ThreadPool::getStatistics(IOStatistics* s)
{
        pthread_mutex_lock(&mutex);
        s->thread.running    = running;
        s->thread.count      = count;
        s->thread.peakCount  = peak;
        s->thread.totalCount = total;
        pthread_mutex_unlock(&mutex);
}
#endif
///////////////////////////////////////////////////////////////////////

ThreadIOHandler::ThreadIOHandler(int minThreads, int maxThreads, int hashSize):
        bufMap(hashSize)
{
        this->minThreads  = minThreads;
        this->maxThreads  = maxThreads;
        this->numActive   = 0;

#if ENABLE_STATISTICS
        this->peakActive  = 0;
        this->totalActive = 0ULL;
#endif

        for(int i = 0; i < this->minThreads; i++) {
                startThread();
        }
}

ThreadIOHandler::~ThreadIOHandler()
{
        shutdown();
}

void
ThreadIOHandler::shutdown()
{
        threadPool.preNotifyStop();
        taskQueue.preNotifyStop();
        taskQueue.clear();
        threadPool.joinAllThreads();
}

bool
ThreadIOHandler::startThread()
{
        if(maxThreads > 0 && threadPool.getCount() >= size_t(maxThreads)) {
                return false;
        }

        addRef();
        if(threadPool.startThread(this, workerThread, this)) {
                return true;
        }
        release();
        return false;
}

void
ThreadIOHandler::workerThread(void* ctx)
{
        ThreadIOHandler* obj = static_cast<ThreadIOHandler*>(ctx);

        int lastfd = -1;

        DBG("@@fd[-1]: workerThread[%08x] - thread started.", (int) pthread_self());

        while(true) {
                IOPlexer*         ioplexer = NULL;
                int               fd       = -1;
                int               events   = 0;
                IOHandlerContext* ctx      = NULL;
                BufferSlot*       slot     = NULL;

                DBG("@@fd[-1]: workerThread[%08x] - try to dequeue, last fd = %d.", (int) pthread_self(), lastfd);

                if(!obj->taskQueue.dequeue(ioplexer, fd, events, ctx, slot)) {
                        DBG("@@fd[-1]: workerThread[%08x] - couldn't dequeue, exit thread, last fd = %d.", (int) pthread_self(), lastfd);
                        break;
                }

                #if ENABLE_STATISTICS
                register size_t numActive = ATOMIC_INCREMENT(obj->numActive);

                if(numActive > obj->peakActive) {
                        obj->peakActive = numActive;
                }
                ATOMIC_INCREMENT64(obj->totalActive);
                #else
                ATOMIC_INCREMENT(obj->numActive);
                #endif

                //NOTE: In current scheduling policy, it follows the rules:
                //1) Each slot (socket connection fd) can only be handled by just one thread at the same time.
                //2) Consecutive events for the same socket will be appended to the pending event list of the slot.
                //3) Each pending event can be rescheduled and appended into task queue, which will be later handled by a worker thread.
                if(slot) {
                        DBG("@@fd[%d]: workerThread[%08x] - start to handle.", fd, (int) pthread_self());

                        //NOTE: Check if the fd has been closed. If fd is invalid,
                        //never try to access it in case that I/O operation could be blocked.
                        if(slot->closed) {
                                DBG("@@fd[%d]: workerThread[%08x] - socket has been closed, skipping IO operations.", fd, (int) pthread_self());
                                goto finished;
                        }

                        ////////////////////////////////////////////////////////////////
                        if((events & (EVENT_CONNECT | EVENT_ACCEPT)) == 0) {
                                DBG("@@fd[%d]: workerThread[%08x] - start to receive data.", fd, (int) pthread_self());

                                while(true) {
                                        char buf[READ_BUFFER_SIZE];
                                        int n = obj->onRecv(ioplexer, fd, buf, sizeof(buf), ctx);

                                        if(n == 0) {
                                                //EOF has met
                                                events |= EVENT_CLOSE;
                                                break;
                                        }
                                        if(n < 0) {
                                                if(errno == EAGAIN) {
                                                        //data are unavailable currently
                                                        break;
                                                }
                                                if(errno == EINTR) {
                                                        //interrupted by signals
                                                        continue;
                                                }
                                                events |= EVENT_CLOSE;
                                                break;
                                        }
                                        slot->incoming.append(buf, n);
                                }
                                DBG("@@fd[%d]: workerThread[%08x] - finish receiving data.", fd, (int) pthread_self());
                        }
                        ////////////////////////////////////////////////////////////////

                        obj->onHandle(ioplexer, fd, events, ctx, slot->incoming, slot->outgoing);

                        ////////////////////////////////////////////////////////////////
                        DBG("@@fd[%d]: workerThread[%08x] - start to send data.", fd, (int) pthread_self());

                        while(true) {
                                char buf[WRITE_BUFFER_SIZE];

                                size_t len  = 0;
                                size_t size = slot->outgoing.fetch(buf, sizeof(buf));

                                if(size == 0) {
                                        obj->onFlush(ioplexer, fd, ctx);
                                        break;
                                }

                                while(len < size) {
                                        int n = obj->onSend(ioplexer, fd, buf + len, size - len, ctx);

                                        if(n == 0) {
                                                //EOF has met
                                                DBG("@@fd[%d]: workerThread[%08x] - EOF while sending data.", fd, (int) pthread_self());
                                                break;
                                        }
                                        if(n < 0) {
                                                switch(errno) {
                                                        case EAGAIN: //send buffer is temporarily unavailable
                                                        case EINTR:  //interrupted by signals
                                                        {        continue;
                                                        }
                                                }
                                                DBG("@@fd[%d]: workerThread[%08x] - error %d while sending data: %m.", fd, (int) pthread_self(), errno);
                                                break;
                                        }
                                        len += n;
                                }
                                slot->outgoing.consume(len);

                                if(len < size) {
                                        obj->onFlush(ioplexer, fd, ctx);
                                        obj->onHandle(ioplexer, fd, EVENT_CLOSE, ctx, slot->incoming, slot->outgoing);
                                        //incomplete outputs due to certain I/O errors
                                        events |= EVENT_CLOSE;
                                        break;
                                }
                        }
                        DBG("@@fd[%d]: workerThread[%08x] - finish sending data.", fd, (int) pthread_self());
                        ////////////////////////////////////////////////////////////////

                        if(events & EVENT_CLOSE) {
                                ioplexer->unset(fd);
                                obj->bufMap.remove(fd);
                        } else {
                                if(events & EVENT_ACCEPT) {
                                        ioplexer->reenable(fd);
                                }
                                obj->taskQueue.reschedule(slot);
                        }

                finished:
                        slot->release();
                        DBG("@@fd[%d]: workerThread[%08x] - finish handling.", fd, (int) pthread_self());
                }
                if(ctx) {
                        ctx->release();
                }

                lastfd = fd;
                ATOMIC_DECREMENT(obj->numActive);
        }
        DBG("@@fd[-1]: workerThread[%08x] - thread terminated.", (int) pthread_self());
        obj->release(); //dereference the object referenced by startThread
}

void
ThreadIOHandler::onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx, Buffer& incoming, Buffer& outgoing)
{
        //overrideable
}

void
ThreadIOHandler::onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx)
{
        BufferSlot* slot = bufMap.acquire(fd);

        if(slot) {
                if(events & EVENT_CONNECT) {
                        DBG("@@fd[%d]: dispatching EVENT_CONNECT - 0x%02x", fd, events);
                        events = EVENT_CONNECT;
                } else if(events & EVENT_ACCEPT) {
                        DBG("@@fd[%d]: dispatching EVENT_ACCEPT - 0x%02x", fd, events);
                        events = EVENT_ACCEPT;
                } else if(events & EVENT_READ) {
                        //events are left untouched
                        DBG("@@fd[%d]: dispatching EVENT_READ - 0x%02x", fd, events);
                } else if(events & EVENT_CLOSE) {
                        DBG("@@fd[%d]: dispatching EVENT_READ + EVENT_CLOSE - 0x%02x", fd, events);
                        events = EVENT_CLOSE | EVENT_READ;
                } else {
                        DBG("@@fd[%d]: event ignored - 0x%02x", fd, events);
                        events = 0;
                }

                if(events) {
                        if(numActive >= threadPool.getCount()) {
                                startThread();
                        }
                        DBG("@@fd[%d]: event enqueued.", fd);
                        taskQueue.enqueue(ioplexer, fd, events, ctx, slot);
                }
                slot->release();
        }
}

int
ThreadIOHandler::onRecv(IOPlexer* ioplexer, int fd, void* buf, size_t len, IOHandlerContext* ctx)
{
        return recv(fd, buf, sizeof(buf), MSG_NOSIGNAL);
}

int
ThreadIOHandler::onSend(IOPlexer* ioplexer, int fd, const void* buf, size_t len, IOHandlerContext* ctx)
{
        return send(fd, buf, len, MSG_NOSIGNAL);
}

void
ThreadIOHandler::onFlush(IOPlexer* ioplexer, int fd, IOHandlerContext* ctx)
{
}

#if ENABLE_STATISTICS
void
ThreadIOHandler::getStatistics(IOStatistics* s)
{
        s->thread.minCount         = minThreads;
        s->thread.maxCount         = maxThreads;
        s->thread.activeCount      = numActive;
        s->thread.peakActiveCount  = peakActive;
        s->thread.totalActiveCount = ATOMIC_INT64(totalActive);

        threadPool.getStatistics(s);
        taskQueue .getStatistics(s);
        bufMap    .getStatistics(s);
}
#endif

/* vim: set ts=8 sts=8 sw=8 ff=unix et: */
