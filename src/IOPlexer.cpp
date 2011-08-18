#include "IOPlexer.h"

#include <fcntl.h>
#include <dlfcn.h>
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <malloc.h>
#include <string.h>
#include <unistd.h>
#include <syslog.h>
#include <sys/epoll.h>
#include <sys/socket.h>

///////////////////////////////////////////////////////////////////////

extern "C" void
__IOPlexerDebugOutput(
        const char* file,
        int         line,
        const char* function,
        const char* fmt, ...);

#define LOG_MODULE      "IOPlexer"
#define DBG(msg...)     __IOPlexerDebugOutput(__FILE__, __LINE__, __FUNCTION__, ##msg)

///////////////////////////////////////////////////////////////////////
IOHandler::IOHandler(
        HandleRoutine  handleRoutine,
        CleanupRoutine cleanupRoutine,
        void*          userData
)
{
        this->refcount       = 1;
        this->handleRoutine  = handleRoutine;
        this->cleanupRoutine = cleanupRoutine;
        this->userData       = userData;
}

IOHandler::~IOHandler()
{
        if(cleanupRoutine) {
                cleanupRoutine(userData);
        }
}

void
IOHandler::shutdown()
{
}

unsigned int
IOHandler::addRef()
{
        return ATOMIC_INCREMENT(refcount);
}

unsigned int
IOHandler::release()
{
        unsigned int result = ATOMIC_DECREMENT(refcount);
        
        if(result == 0) {
                delete this;
                return 0;
        }
        return result;
}

void
IOHandler::onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx)
{
        if(handleRoutine && ioplexer) {
                handleRoutine(ioplexer, fd, userData, events);
        }
}

#if ENABLE_STATISTICS
void
IOHandler::getStatistics(IOStatistics* statistics)
{
}
#endif

///////////////////////////////////////////////////////////////////////
IOHandlerContext::IOHandlerContext(void* ctx)
{
        this->ctx      = ctx;
        this->refcount = 1;
}

IOHandlerContext::~IOHandlerContext()
{
}

unsigned int
IOHandlerContext::addRef()
{
        return ATOMIC_INCREMENT(refcount);
}

unsigned int
IOHandlerContext::release()
{
        unsigned int result = ATOMIC_DECREMENT(refcount);
        if(result == 0) {
                delete this;
                return 0;
        }
        return result;
}

void*
IOHandlerContext::getContext() const
{
        return ctx;
}

void
IOHandlerContext::setContext(void* ctx)
{
        this->ctx = ctx;
}

///////////////////////////////////////////////////////////////////////
IOPlexer::IORenewRequest::IORenewRequest(int fd)
{
        this->prev = NULL;
        this->next = NULL;
        this->fd   = fd;
}

IOPlexer::IORenewRequest::~IORenewRequest()
{
}

///////////////////////////////////////////////////////////////////////
IOPlexer::IOHandlerSlot::IOHandlerSlot(int fd)
{       this->next       = NULL;
        this->fd         = fd;
        this->eventMask  = 0;
        this->handler    = NULL;
        this->ctx        = NULL;
        this->touched    = 0ULL;
}

IOPlexer::IOHandlerSlot::~IOHandlerSlot()
{
        if(handler) {
                handler->release();
        }
        if(ctx) {
                ctx->release();
        }
        closeFd();
}

IOHandler*
IOPlexer::IOHandlerSlot::setHandler(IOHandler* handler)
{
        register IOHandler* lastHandler = NULL;

        if(this->handler != handler) {
                lastHandler   = this->handler;
                this->handler = handler;

                if(this->handler) {
                        this->handler->addRef();
                }
        }
        return lastHandler;
}

IOHandlerContext*
IOPlexer::IOHandlerSlot::setContext(IOHandlerContext* ctx)
{
        register IOHandlerContext* lastCtx = NULL;

        if(this->ctx != ctx) {
                lastCtx       = this->ctx;
                this->ctx     = ctx;

                if(this->ctx) {
                        this->ctx->addRef();
                }
        }
        return lastCtx;
}

void
IOPlexer::IOHandlerSlot::closeFd()
{
        if(fd >= 0) {
                if(::close(fd) < 0) {
                        switch(errno) {
                                case EBADF:
                                {       DBG("@@fd[%d]: closeFd() - bad descriptor for close.", fd);
                                        break;
                                }
                                case EINTR:
                                {       DBG("@@fd[%d]: closeFd() - interrupted for close.", fd);
                                        break;
                                }
                                case EIO:
                                {       DBG("@@fd[%d]: closeFd() - error io for close.", fd);
                                        break;
                                }
                                default:
                                {       DBG("@@fd[%d] closeFd() - unknown error occurred for close, errno = %d.", fd, errno);
                                        break;
                                }
                        }
                } else {
                        DBG("@@fd[%d]: closeFd() - close successfully.", fd);
                }
                fd = -1;
        }
}
///////////////////////////////////////////////////////////////////////

IOPlexer::IOPlexer(int eventCapacity, int hashSize)
{
        pthread_mutex_init(&mutex, NULL);

        if(eventCapacity <= 0) {
                eventCapacity = DEFAULT_EVENT_CAPACITY;
        }
        if(hashSize <= 0) {
                hashSize = DEFAULT_HASH_SIZE;
        }

        this->handlerMap    = NULL;
        this->hashSize      = 0;
        this->fdCount       = 0;
        this->peakFdCount   = 0;
        this->totalFdCount  = 0;
        this->events        = NULL;
        this->eventCapacity = 0;
        this->eventCount    = 0;
        this->lastReceived  = 0;
        this->handle        = epoll_create(eventCapacity);
        this->requestHead   = NULL;
        this->requestTail   = NULL;

        if(this->handle >= 0) {
                int flags = fcntl(this->handle, F_GETFD, 0);

                if(flags >= 0) {
                        fcntl(this->handle, F_SETFD, flags | FD_CLOEXEC);
                }

                this->handlerMap    = (IOHandlerSlot**) calloc(hashSize, sizeof(IOHandlerSlot*));
                this->hashSize      = hashSize;
                this->eventCapacity = eventCapacity;
        } else {
                switch(errno) {
                        case EINVAL:    //size is not positive.
                        case ENFILE:    //The system limit on the total number of open files has been reached.
                        case ENOMEM:    //There was insufficient memory to create the kernel object.
                        {       break;
                        }
                }
        }
        #if ENABLE_STATISTICS
        this->curAcceptEvents    = 0;
        this->curConnectEvents   = 0;
        this->curReadEvents      = 0;
        this->curWriteEvents     = 0;
        this->curCloseEvents     = 0;
        this->peakAcceptEvents   = 0;
        this->peakConnectEvents  = 0;
        this->peakReadEvents     = 0;
        this->peakWriteEvents    = 0;
        this->peakCloseEvents    = 0;
        this->totalAcceptEvents  = 0ULL;
        this->totalConnectEvents = 0ULL;
        this->totalReadEvents    = 0ULL;
        this->totalWriteEvents   = 0ULL;
        this->totalCloseEvents   = 0ULL;
        #endif
}

IOPlexer::~IOPlexer()
{
        clear();

        pthread_mutex_lock(&mutex);

        if(events) {
                free(events);
                events     = NULL;
                eventCount = 0;
        }
        if(handlerMap) {
                free(handlerMap);
                handlerMap = NULL;
        }
        if(handle >= 0) {
                close(handle);
                handle = -1;
        }
        pthread_mutex_unlock(&mutex);
        pthread_mutex_destroy(&mutex);
}

void
IOPlexer::clear()
{
        pthread_mutex_lock(&mutex);

        if(handlerMap) {
                for(size_t i = 0; i < hashSize; i++) {
                        register IOHandlerSlot* slot = handlerMap[i];

                        while(slot) {
                                handlerMap[i] = slot->next;
                                fdCount--;
                                pthread_mutex_unlock(&mutex);

                                delete slot;

                                pthread_mutex_lock(&mutex);
                                slot = handlerMap[i];
                        }
                }
        }
        lastReceived = 0;

        register IORenewRequest* p = requestHead;
        while(p) {
                register IORenewRequest* next = p->next;
                delete p;
                p = next;
        }
        requestHead = NULL;
        requestTail = NULL;

        pthread_mutex_unlock(&mutex);
}

bool
IOPlexer::set(int fd, int eventMask, IOHandler* handler, IOHandlerContext* ctx)
{
        pthread_mutex_lock(&mutex);

        if(handlerMap == NULL || handle < 0) {
                //initiaization failure
                pthread_mutex_unlock(&mutex);
                return false;
        }
        if(fd < 0) {
                //invalid fd
                pthread_mutex_unlock(&mutex);
                return false;
        }

        register size_t         ndx  = hash(fd);
        register IOHandlerSlot* slot = handlerMap[ndx];

        ////////////////////////////////////////////////////////////////////////
        //unset the event monitor if no routine specified

        if(handler == NULL) {
                register IOHandlerSlot* prev = NULL;

                for(; slot; prev = slot, slot = slot->next) {
                        if(slot->fd == fd) {
                                if(prev) {
                                        prev->next = slot->next;
                                } else {
                                        handlerMap[ndx] = slot->next;
                                }
                                fdCount--;
                                pthread_mutex_unlock(&mutex);

                                delete slot;
                                return true;
                        }
                }
                pthread_mutex_unlock(&mutex);
                return true;
        }

        ////////////////////////////////////////////////////////////////////////
        eventMask &= IOHandler::EVENT_ALL;

        if(eventMask == 0) {
                //invalid mask
                pthread_mutex_unlock(&mutex);
                return false;
        }

        int flags = fcntl(fd, F_GETFL, 0);

        if(flags < 0) {
                DBG("@@fd[%d]: fcntl(F_GETFL) returned errno = %d.", fd, errno);
                pthread_mutex_unlock(&mutex);
                return false;
        }
        if(fcntl(fd, F_SETFL, flags | O_NONBLOCK | FD_CLOEXEC) < 0) {
                DBG("@@fd[%d]: fcntl(F_SETFL) returned errno = %d.", fd, errno);
                pthread_mutex_unlock(&mutex);
                return false;
        }

        for(; slot; slot = slot->next) {
                if(slot->fd == fd) {
                        break;
                }
        }

        epoll_event e;
        memset(&e, 0, sizeof(e));

        if(eventMask & (IOHandler::EVENT_ACCEPT)) {
                e.events |= EPOLLIN | EPOLLONESHOT;
        } else {
                if(eventMask & (IOHandler::EVENT_READ)) {
                        e.events |= EPOLLIN | EPOLLET;
                }
                if(eventMask & IOHandler::EVENT_WRITE) {
                        e.events |= EPOLLOUT | EPOLLET;
                }
        }

        e.data.fd = fd;

        if(epoll_ctl(handle, EPOLL_CTL_ADD, fd, &e) < 0) {
                switch(errno) {
                        case EEXIST:    //op was EPOLL_CTL_ADD, and the supplied file descriptor fd is already in epfd.
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned EEXIST, should redo EPOLL_CTL_MOD.", fd);
                                epoll_ctl(handle, EPOLL_CTL_MOD, fd, &e);
                                break;
                        }
                        case EBADF:     //epfd or fd is not a valid file descriptor.
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned BADF.", fd);
                                goto unlock;
                        }
                        case EINVAL:    //epfd is not an epoll file descriptor, or fd is the same as epfd, or the requested operation op is not supported by this interface.
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned EINVAL.", fd);
                                goto unlock;
                        }
                        case ENOENT:    //op was EPOLL_CTL_MOD or EPOLL_CTL_DEL, and fd is not in epfd.
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned ENOENT.", fd);
                                goto unlock;
                        }
                        case ENOMEM:    //There was insufficient memory to handle the requested op control operation.
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned ENOMEM.", fd);
                                goto unlock;
                        }
                        case EPERM:     //The target file fd does not support epoll.
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned EPERM.", fd);
                                goto unlock;
                        }
                        default:
                        {       DBG("@@fd[%d]: EPOLL_CTL_ADD returned errno = %d.", fd, errno);
                                //pass thru
                        }
                        unlock:
                        {
                                pthread_mutex_unlock(&mutex);
                                return false;
                        }
                }
        }

        if(slot == NULL) {
                slot = new IOHandlerSlot(fd);

                if(slot == NULL) {
                        //insufficient resource
                        epoll_ctl(handle, EPOLL_CTL_DEL, fd, NULL);
                        pthread_mutex_unlock(&mutex);
                        return false;
                }
                slot->next      = handlerMap[ndx];
                handlerMap[ndx] = slot;
                fdCount++;
                totalFdCount++;

                if(fdCount > peakFdCount) {
                        peakFdCount = fdCount;
                }
        }
        slot->eventMask = eventMask;

        register IOHandler*        lastHandler = slot->setHandler(handler);
        register IOHandlerContext* lastCtx     = slot->setContext(ctx); 

        pthread_mutex_unlock(&mutex);

        if(lastHandler) {
                lastHandler->release();
        }
        if(lastCtx) {
                lastCtx->release();
        }
        return true;
}

bool
IOPlexer::set(
        int                       fd,
        int                       eventMask,
        IOHandler::HandleRoutine  handleRoutine,
        IOHandler::CleanupRoutine cleanupRoutine,
        void*                     userData
)
{
        bool       result  = false;
        IOHandler* handler = new IOHandler(handleRoutine, cleanupRoutine, userData);

        if(handler) {
                result = set(fd, eventMask, handler, NULL);
                handler->release();
        }
        return result;
}

bool
IOPlexer::unset(int fd)
{
        return set(fd, 0, (IOHandler*) NULL, (IOHandlerContext*) NULL);
}

bool
IOPlexer::reenable(int fd)
{
        pthread_mutex_lock(&mutex);

        bool                     result = false;
        register IORenewRequest* p      = requestHead;

        for(; p; p = p->next) {
                if(p->fd == fd) {
                        result = true;
                        DBG("@@fd[%d]: re-enable has been already requested.", fd);
                        break;
                }
        }

        if(p == NULL) {
                p = new IORenewRequest(fd);

                if(p) {
                        if(requestHead) {
                                p->prev = requestTail;
                                requestTail = requestTail->next = p;
                        } else {
                                requestHead = requestTail = p;
                        }
                        result = true;
                        DBG("@@fd[%d]: requested to re-enable.", fd);
                }
        }
        pthread_mutex_unlock(&mutex);
        return result;
}

bool
IOPlexer::raise(int fd, int events)
{
        pthread_mutex_lock(&mutex);

        if(handlerMap == NULL || handle < 0) {
                //initialization failure
                pthread_mutex_unlock(&mutex);
                return false;
        }

        events &= IOHandler::EVENT_ALL;

        register int            ndx  = hash(fd);
        register IOHandlerSlot* slot = handlerMap[ndx];

        for(; slot; slot = slot->next) {
                if(slot->fd == fd) {
                        if(slot->handler) {
                                IOHandler*        handler = slot->handler;
                                IOHandlerContext* ctx     = slot->ctx;

                                if(ctx) {
                                        ctx->addRef();
                                }

                                handler->addRef();
                                pthread_mutex_unlock(&mutex);
                                handler->onHandle(this, fd, events, ctx);
                                handler->release();

                                if(ctx) {
                                        ctx->release();
                                }
                                return true;
                        }
                        break;
                }
        }
        pthread_mutex_unlock(&mutex);
        return false;
}

bool
IOPlexer::dispatch(int idleTimeoutMillis)
{
        bool result = false;

        #if ENABLE_STATISTICS
        curAcceptEvents  = 0;
        curConnectEvents = 0;
        curReadEvents    = 0;
        curWriteEvents   = 0;
        curCloseEvents   = 0;
        #endif

        pthread_mutex_lock(&mutex);

        if(handlerMap == NULL || handle < 0) {
                //initialization failure
                pthread_mutex_unlock(&mutex);
                goto leave;
        }

        if(events == NULL || eventCount <= 0 || lastReceived <= 0 || (lastReceived == eventCount && eventCount < eventCapacity)) {
                uint32_t new_count = lastReceived * 2;

                if(new_count <= 0) {
                        new_count = INIT_EVENT_COUNT;
                } else if(new_count >= eventCapacity) {
                        new_count = eventCapacity;
                }

                epoll_event* new_events = (epoll_event*) realloc(events, new_count * sizeof(epoll_event));

                if(new_events == NULL) {
                        //insufficient memory
                        pthread_mutex_unlock(&mutex);
                        goto leave;
                }
                events     = new_events;
                eventCount = new_count;
        }

        register IORenewRequest* p = requestHead;

        while(p) {
                register IORenewRequest* next = p->next;

                register int             ndx  = hash(p->fd);
                register IOHandlerSlot*  slot = handlerMap[ndx];

                for(; slot; slot = slot->next) {
                        if(slot->fd == p->fd) {
                                epoll_event e;
                                memset(&e, 0, sizeof(e));

                                if(slot->eventMask & (IOHandler::EVENT_ACCEPT)) {
                                        e.events |= EPOLLIN | EPOLLONESHOT;
                                } else {
                                        if(slot->eventMask & (IOHandler::EVENT_READ)) {
                                                e.events |= EPOLLIN | EPOLLET;
                                        }
                                        if(slot->eventMask & IOHandler::EVENT_WRITE) {
                                                e.events |= EPOLLOUT | EPOLLET;
                                        }
                                }

                                e.data.fd = p->fd;

                                if(epoll_ctl(handle, EPOLL_CTL_MOD, p->fd, &e) >= 0) {
                                        DBG("@@fd[%d]: re-enabled successfully with events: [%s%s%s%s]", p->fd,
                                                e.events & EPOLLIN     ? "+EPOLLIN"     : "",
                                                e.events & EPOLLOUT    ? "+EPOLLOUT"    : "",
                                                e.events & EPOLLET     ? "+EPOLLET"     : "",
                                                e.events & EPOLLONESHOT? "+EPOLLONESHOT": "");
                                } else {
                                        switch(errno) {
                                                case EEXIST:    //op was EPOLL_CTL_ADD, and the supplied file descriptor fd is already in epfd.
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned EEXIST.", p->fd);
                                                        break;
                                                }
                                                case EBADF:     //epfd or fd is not a valid file descriptor.
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned BADF.", p->fd);
                                                        break;
                                                }
                                                case EINVAL:    //epfd is not an epoll file descriptor, or fd is the same as epfd, or the requested operation op is not supported by this interface.
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned EINVAL.", p->fd);
                                                        break;
                                                }
                                                case ENOENT:    //op was EPOLL_CTL_MOD or EPOLL_CTL_DEL, and fd is not in epfd.
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned ENOENT.", p->fd);
                                                        break;
                                                }
                                                case ENOMEM:    //There was insufficient memory to handle the requested op control operation.
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned ENOMEM.", p->fd);
                                                        break;
                                                }
                                                case EPERM:     //The target file fd does not support epoll.
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned EPERM.", p->fd);
                                                        break;
                                                }
                                                default:
                                                {       DBG("@@fd[%d]: failed to re-enable, EPOLL_CTL_MOD returned errno = %d.", p->fd, errno);
                                                        break;
                                                }
                                        }
                                }
                                break;
                        }
                }

                delete p;
                p = next;
        }
        requestHead = NULL;
        requestTail = NULL;

        pthread_mutex_unlock(&mutex);

        while(true) {
                int num = epoll_wait(handle, static_cast<epoll_event*>(events), eventCount, idleTimeoutMillis);

                if(num > 0) {
                        pthread_mutex_lock(&mutex);
                        lastReceived = num;
                        break;
                }

                if(num == 0) {
                        //timeout expired
                        goto leave;
                }

                switch(errno) {
                        default:
                        case EBADF:     //epfd is not a valid file descriptor.
                        case EFAULT:    //The memory area pointed to by events is not accessible with write permissions.
                        case EINVAL:    //epfd is not an epoll file descriptor, or maxevents is less than or equal to zero.
                        {       DBG("@@fd[%d]: epoll_wait returned errno = %d.", handle, errno);
                                goto leave;
                        }
                        case EINTR:     //The call was interrupted by a signal handler before any of the requested events occurred or the timeout expired.
                        {       goto leave;
                        }
                }
        }

        for(size_t i = 0; i < lastReceived; i++) {
                register int fd = static_cast<epoll_event*>(events)[i].data.fd;

                if(fd < 0) {
                        continue;
                }

                register int            triggered = static_cast<epoll_event*>(events)[i].events;
                register int            ndx       = hash(fd);
                register IOHandlerSlot* slot      = handlerMap[ndx];
                register IOHandlerSlot* prev      = NULL;

                for(; slot; prev = slot, slot = slot->next) {
                        if(slot->fd == fd) {
                                if(slot->handler) {
                                        IOHandler*        handler   = slot->handler;
                                        IOHandlerContext* ctx       = slot->ctx;
                                        uint64_t          touched   = slot->touched++;
                                        int               eventMask = 0;

                                        if(triggered & (EPOLLHUP | EPOLLERR)) {
                                                eventMask |= IOHandler::EVENT_CLOSE;
                                        } else {
                                                if(triggered & EPOLLIN) {
                                                        eventMask |= (IOHandler::EVENT_READ | IOHandler::EVENT_ACCEPT);
                                                }
                                                if(triggered & EPOLLOUT) {
                                                        eventMask |= IOHandler::EVENT_WRITE;
                                                }
                                        }

                                        if(touched == 0) {
                                                eventMask |= IOHandler::EVENT_CONNECT;
                                        }

                                        eventMask &= slot->eventMask;

                                        #if ENABLE_STATISTICS
                                        if(eventMask & IOHandler::EVENT_ACCEPT) {
                                                curAcceptEvents++;
                                                totalAcceptEvents++;
                                                if(curAcceptEvents > peakAcceptEvents) {
                                                        peakAcceptEvents = curAcceptEvents;
                                                }
                                        }
                                        if(eventMask & IOHandler::EVENT_CONNECT) {
                                                curConnectEvents++;
                                                totalConnectEvents++;
                                                if(curConnectEvents > peakConnectEvents) {
                                                        peakConnectEvents = curConnectEvents;
                                                }
                                        }
                                        if(eventMask & IOHandler::EVENT_READ) {
                                                curReadEvents++;
                                                totalReadEvents++;
                                                if(curReadEvents > peakReadEvents) {
                                                        peakReadEvents = curReadEvents;
                                                }
                                        }
                                        if(eventMask & IOHandler::EVENT_WRITE) {
                                                curWriteEvents++;
                                                totalWriteEvents++;
                                                if(curWriteEvents > peakWriteEvents) {
                                                        peakWriteEvents = curWriteEvents;
                                                }
                                        }
                                        if(eventMask & IOHandler::EVENT_CLOSE) {
                                                curCloseEvents++;
                                                totalCloseEvents++;
                                                if(curCloseEvents > peakCloseEvents) {
                                                        peakCloseEvents = curCloseEvents;
                                                }
                                        }
                                        #endif

                                        if(ctx) {
                                                ctx->addRef();
                                        }

                                        handler->addRef();
                                        pthread_mutex_unlock(&mutex);

                                        if(eventMask & IOHandler::EVENT_CONNECT) {
                                                handler->onHandle(this, fd, IOHandler::EVENT_CONNECT, ctx);
                                        }

                                        if(eventMask & IOHandler::EVENT_CLOSE) {
                                                eventMask |= IOHandler::EVENT_READ | IOHandler::EVENT_WRITE;
                                        }

                                        handler->onHandle(this, fd, eventMask & ~IOHandler::EVENT_CONNECT, ctx);
                                        handler->release();

                                        if(ctx) {
                                                ctx->release();
                                        }
                                        pthread_mutex_lock(&mutex);
                                }
                                break;
                        }
                }
        }
        pthread_mutex_unlock(&mutex);
        result = true;
leave:
        return result;
}

void
IOPlexer::loop(int idleTimeoutMillis)
{
        while(true) {
                if(!dispatch(idleTimeoutMillis)) {
                        break;
                }
        }
}

size_t 
IOPlexer::hash(int fd)
{
        return size_t(fd % hashSize);
}

#if ENABLE_STATISTICS
void
IOPlexer::getStatistics(IOStatistics* s)
{
        pthread_mutex_lock(&mutex);

        memset(s, 0, sizeof(*s));
        s->event.capacity           = eventCapacity;
        s->event.count              = eventCount;
        s->event.curAcceptEvents    = curAcceptEvents;
        s->event.curConnectEvents   = curConnectEvents;
        s->event.curReadEvents      = curReadEvents;
        s->event.curWriteEvents     = curWriteEvents;
        s->event.curCloseEvents     = curCloseEvents;
        s->event.peakAcceptEvents   = peakAcceptEvents;
        s->event.peakConnectEvents  = peakConnectEvents;
        s->event.peakReadEvents     = peakReadEvents;
        s->event.peakWriteEvents    = peakWriteEvents;
        s->event.peakCloseEvents    = peakCloseEvents;
        s->event.totalAcceptEvents  = totalAcceptEvents;
        s->event.totalConnectEvents = totalConnectEvents;
        s->event.totalReadEvents    = totalReadEvents;
        s->event.totalWriteEvents   = totalWriteEvents;
        s->event.totalCloseEvents   = totalCloseEvents;

        s->hash.size                = hashSize;
        s->hash.fdCount             = fdCount;
        s->hash.peakFdCount         = peakFdCount;
        s->hash.totalFdCount        = totalFdCount;

        if(handlerMap) {
                for(size_t ndx = 0; ndx < hashSize; ndx++) {
                        IOHandlerSlot* slot  = handlerMap[ndx];
                        size_t         depth = 0;

                        for(; slot; slot = slot->next, depth++);

                        if(depth > s->hash.maxCollision) {
                                s->hash.maxCollision = depth;
                        }
                }
        }
        pthread_mutex_unlock(&mutex);
}
#endif

///////////////////////////////////////////////////////////////////////////////////

static void* logger_dll    = NULL;
static void* logger_handle = NULL;
static void* (*logger_open )(const char* module_name, const char* conf, int *ret) = NULL;
static int   (*logger_close)(void *handle) = NULL;
static int   (*logger_send )(void *handle, int level, void *buf, int len) = NULL;

extern "C" void
__IOPlexerDebugOutput(
        const char* file,
        int         line,
        const char* function,
        const char* fmt, ...)
{
        if(logger_dll) {
                if(logger_handle == NULL) {
                        return;
                }
        }

        char buf[4096] = "";
        int  n         = 0;
        int  space     = sizeof(buf);

        if(space > 0) {
                n += snprintf(buf + n, space, "[%s] ", LOG_MODULE), space -= n;
        }
        if(space > 0) {
                va_list args;
                va_start(args, fmt);
                n += vsnprintf(buf + n, space, fmt, args), space -= n;
                va_end(args);
        }
        if(space > 0) {
                n += snprintf(buf + n, space, "\t[%s@%s:%d]", function, file, line), space -= n;
        }

        if(logger_dll) {
                logger_send(logger_handle, LOG_DEBUG, buf, n);
        } else {
                syslog(LOG_DEBUG, "%s", buf);
        }
}

void __attribute__((constructor))
hook_init()
{
        logger_dll = dlopen("libtmsyslog.so", RTLD_LAZY);

        if(logger_dll) {
                *((void**) &logger_open ) = dlsym(logger_dll, "tmSyslog_openlog" );
                *((void**) &logger_close) = dlsym(logger_dll, "tmSyslog_closelog");
                *((void**) &logger_send ) = dlsym(logger_dll, "tmSyslogEx"       );

                if(logger_open && logger_close && logger_send) {
                        logger_handle = logger_open(LOG_MODULE, NULL, NULL);
                } else {
                        dlclose(logger_dll);
                        logger_open  = NULL;
                        logger_close = NULL;
                        logger_send  = NULL;
                        logger_dll   = NULL;
                }
        }
}

void __attribute__((destructor))
hook_uninit()
{
        if(logger_handle) {
                logger_close(logger_handle);
                logger_handle = NULL;
        }
        if(logger_dll) {
                dlclose(logger_dll);
                logger_open  = NULL;
                logger_close = NULL;
                logger_send  = NULL;
                logger_dll   = NULL;
        }
}

/* vim: set ts=8 sts=8 sw=8 ff=unix et: */

