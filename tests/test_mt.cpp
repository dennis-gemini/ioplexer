#include "IOPlexer.h"
#include "ThreadIOHandler.h"

#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>
#include <errno.h>

int 
accept_socket()
{
        int sd = socket(AF_INET, SOCK_STREAM, 0);

        if(sd < 0) {
                perror("socket");
                return -1;
        }

        int opt = 1;
        if(setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
                close(sd);
                perror("setsockopt");
                return -1;
        }

        struct sockaddr_in addr;
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = 0;
        addr.sin_port        = htons(1234);

        if(bind(sd, (struct sockaddr*) &addr, sizeof(addr)) < 0) {
                close(sd);
                perror("bind");
                return -1;
        }

        if(listen(sd, 4096) < 0) {
                close(sd);
                perror("listen");
                return -1;
        }
        return sd;
}

class ReadWriteHandler: public ThreadIOHandler
{
public:
        ReadWriteHandler()
        {
        }

protected:
        virtual
        ~ReadWriteHandler()
        {
        }

        virtual void
        onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx, Buffer& incoming, Buffer& outgoing)
        {
                if(events & EVENT_CONNECT) {
                        fprintf(stderr, "connected!\n");
                } else if(events & EVENT_CLOSE) {
                        fprintf(stderr, "connection closed!\n"); 
                } else if(events & EVENT_READ) {
                        char buf[65536];

                        size_t n = incoming.fetch(buf, sizeof(buf));
                        incoming.consume(n);
                        outgoing.append(buf, n);
                }
        }
};

class AcceptHandler: public IOHandler
{
        ReadWriteHandler* readWriteHandler;

public:
        AcceptHandler()
        {
                readWriteHandler = new ReadWriteHandler;
        }

#if ENABLE_STATISTICS
        virtual void
        getStatistics(IOStatistics* s)
        {       readWriteHandler->getStatistics(s);
        }
#endif

protected:
        virtual void
        onHandle(IOPlexer* ioplexer, int fd, int events, IOHandlerContext* ctx/*, Buffer&, Buffer&*/)
        {
                if(events & EVENT_ACCEPT) {

                        int default_tries = 10;
                        int tries = default_tries;
                        while(tries > 0) {
                                struct sockaddr_in addr;
                                socklen_t len = sizeof(addr);

                                int client = accept(fd, (sockaddr*)&addr, &len);

                                if(client < 0) {
                                        if(errno != EAGAIN) {
                                                perror("accept");
                                                return;
                                        }
                                        tries--;
                                        continue;
                                }

                                if(ioplexer->set(client, IOHandler::EVENT_CONNECT | IOHandler::EVENT_READWRITE | IOHandler::EVENT_CLOSE, readWriteHandler)) {
                                        //printf("accepted[%d]\n", client);
                                        tries = default_tries;
                                } else {
                                        close(client);
                                        perror("ioplexer");
                                }
                        }
                }
        }

        virtual
        ~AcceptHandler()
        {
                readWriteHandler->shutdown();
                readWriteHandler->release();
        }
};

int
main(int argc, char** argv)
{
        int sd = accept_socket();

        if(sd < 0) {
                return 1;
        } else {
                AcceptHandler* acceptHandler = new AcceptHandler();

                IOPlexer ioplexer;
                if(!ioplexer.set(sd, IOHandler::EVENT_ACCEPT, acceptHandler)) {
                        perror("ioplexer");
                } else {
#if ENABLE_STATISTICS
                        IOStatistics last;
                        memset(&last, 0, sizeof(last));

                        struct timeval start;
                        gettimeofday(&start, NULL);
#endif

                        while(true) {
                                if(!ioplexer.dispatch(30000)) {
                                        break;
                                }

#if ENABLE_STATISTICS
                                struct timeval now;
                                gettimeofday(&now, NULL);

                                if(now.tv_sec - start.tv_sec < 1) {
                                        continue;
                                }

                                IOStatistics cur;
                                ioplexer.getStatistics(&cur);
                                acceptHandler->getStatistics(&cur);

                                #define DATA(var)       (cur. var), ssize_t((cur. var) - (last. var))
                                printf("------------ start of statistics ------------\n");
                                //printf("event.capacity                  = %u (%d)\n"  , DATA(event.capacity                 ));
                                printf("event.count                     = %u (%d)\n"  , DATA(event.count                    ));
                                printf("event.curAcceptEvents           = %u (%d)\n"  , DATA(event.curAcceptEvents          ));
                                printf("event.curConnectEvents          = %u (%d)\n"  , DATA(event.curConnectEvents         ));
                                printf("event.curReadEvents             = %u (%d)\n"  , DATA(event.curReadEvents            ));
                                printf("event.curWriteEvents            = %u (%d)\n"  , DATA(event.curWriteEvents           ));
                                printf("event.curCloseEvents            = %u (%d)\n"  , DATA(event.curCloseEvents           ));
                                printf("event.peakAcceptEvents          = %u (%d)\n"  , DATA(event.peakAcceptEvents         ));
                                printf("event.peakConnectEvents         = %u (%d)\n"  , DATA(event.peakConnectEvents        ));
                                printf("event.peakReadEvents            = %u (%d)\n"  , DATA(event.peakReadEvents           ));
                                printf("event.peakWriteEvents           = %u (%d)\n"  , DATA(event.peakWriteEvents          ));
                                printf("event.peakCloseEvents           = %u (%d)\n"  , DATA(event.peakCloseEvents          ));
                                printf("event.totalAcceptEvents         = %llu (%d)\n", DATA(event.totalAcceptEvents        ));
                                printf("event.totalConnectEvents        = %llu (%d)\n", DATA(event.totalConnectEvents       ));
                                printf("event.totalReadEvents           = %llu (%d)\n", DATA(event.totalReadEvents          ));
                                printf("event.totalWriteEvents          = %llu (%d)\n", DATA(event.totalWriteEvents         ));
                                printf("event.totalCloseEvents          = %llu (%d)\n", DATA(event.totalCloseEvents         ));
                                //printf("hash.size                       = %u (%d)\n"  , DATA(hash.size                      ));
                                printf("hash.fdCount                    = %u (%d)\n"  , DATA(hash.fdCount                   ));
                                printf("hash.peakFdCount                = %u (%d)\n"  , DATA(hash.peakFdCount               ));
                                printf("hash.totalFdCount               = %llu (%d)\n", DATA(hash.totalFdCount              ));
                                printf("hash.maxCollision               = %u (%d)\n"  , DATA(hash.maxCollision              ));
                                printf("thread.running                  = %u (%d)\n"  , DATA(thread.running                 ));
                                //printf("thread.minCount                 = %u (%d)\n"  , DATA(thread.minCount                ));
                                //printf("thread.maxCount                 = %u (%d)\n"  , DATA(thread.maxCount                ));
                                printf("thread.count                    = %u (%d)\n"  , DATA(thread.count                   ));
                                printf("thread.peakCount                = %u (%d)\n"  , DATA(thread.peakCount               ));
                                printf("thread.activeCount              = %u (%d)\n"  , DATA(thread.activeCount             ));
                                printf("thread.peakActiveCount          = %u (%d)\n"  , DATA(thread.peakActiveCount         ));
                                printf("thread.totalCount               = %llu (%d)\n", DATA(thread.totalCount              ));
                                printf("taskqueue.size                  = %u (%d)\n"  , DATA(taskqueue.size                 ));
                                printf("taskqueue.peakSize              = %u (%d)\n"  , DATA(taskqueue.peakSize             ));
                                printf("taskqueue.totalSize             = %llu (%d)\n", DATA(taskqueue.totalSize            ));
                                //printf("buffer.hashSize                 = %u (%d)\n"  , DATA(buffer.hashSize                ));
                                printf("buffer.slotCount                = %u (%d)\n"  , DATA(buffer.slotCount               ));
                                printf("buffer.peakSlotCount            = %u (%d)\n"  , DATA(buffer.peakSlotCount           ));
                                printf("buffer.totalSlotCount           = %llu (%d)\n", DATA(buffer.totalSlotCount          ));
                                printf("buffer.incoming.size            = %u (%d)\n"  , DATA(buffer.incoming.size           ));
                                printf("buffer.incoming.peakSize        = %u (%d)\n"  , DATA(buffer.incoming.peakSize       ));
                                printf("buffer.incoming.totalSize       = %llu (%d)\n", DATA(buffer.incoming.totalSize      ));
                                printf("buffer.incoming.chunkCount      = %u (%d)\n"  , DATA(buffer.incoming.chunkCount     ));
                                printf("buffer.incoming.peakChunkCount  = %u (%d)\n"  , DATA(buffer.incoming.peakChunkCount ));
                                printf("buffer.incoming.totalChunkCount = %llu (%d)\n", DATA(buffer.incoming.totalChunkCount));
                                printf("buffer.outgoing.size            = %u (%d)\n"  , DATA(buffer.outgoing.size           ));
                                printf("buffer.outgoing.peakSize        = %u (%d)\n"  , DATA(buffer.outgoing.peakSize       ));
                                printf("buffer.outgoing.totalSize       = %llu (%d)\n", DATA(buffer.outgoing.totalSize      ));
                                printf("buffer.outgoing.chunkCount      = %u (%d)\n"  , DATA(buffer.outgoing.chunkCount     ));
                                printf("buffer.outgoing.peakChunkCount  = %u (%d)\n"  , DATA(buffer.outgoing.peakChunkCount ));
                                printf("buffer.outgoing.totalChunkCount = %llu (%d)\n", DATA(buffer.outgoing.totalChunkCount));
                                printf("------------- end of statistics -------------\n");

                                memcpy(&last, &cur, sizeof(cur));
                                start = now;
#endif
                        }
                        printf("exit dispatch loop\n");
                }
                acceptHandler->release();
                close(sd);
        }
        return 0;
}

/* vim: set ts=8 sts=8 sw=8 ff=unix et: */
