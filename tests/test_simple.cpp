#include "IOPlexer.h"

#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>

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

void
do_readwrite(IOPlexer* ioplexer, int fd, void* ctx, int events)
{
        if(events & IOHandler::EVENT_CONNECT) {
                printf("connected[%d]\n", fd);
                return;
        }
        if(events & IOHandler::EVENT_CLOSE) {
                printf("connection closed[%d]\n", fd);
                return;
        }
        if(events & IOHandler::EVENT_READ) {
                char buf[1024];
                int n = recv(fd, buf, sizeof(buf), 0);

                if(n <= 0) {
                        ioplexer->unset(fd);
                        close(fd);
                        printf("read-hup[%d]\n", fd);
                        return;
                }
                buf[n] = '\0';
                printf("read[%d]: %d %s\n", fd, n, buf);
                return;
        }
        if(events & IOHandler::EVENT_WRITE) {
                char buf[1024];
                snprintf(buf, sizeof(buf), "Welcome fd=%d\n", fd);

                int n = send(fd, buf, strlen(buf), 0);

                if(n <= 0) {
                        ioplexer->unset(fd);
                        close(fd);
                        printf("write-hup[%d]\n", fd);
                        return;
                }
                printf("write[%d]: %d\n", fd, n);
                return;
        }
}

void
do_accept(IOPlexer* ioplexer, int fd, void* ctx, int mask)
{
        if(mask & IOHandler::EVENT_READ) {
                struct sockaddr_in addr;
                socklen_t len = sizeof(addr);

                int client = accept(fd, (sockaddr*)&addr, &len);

                if(client < 0) {
                        perror("accept");
                        return;
                }

                if(ioplexer->set(client, IOHandler::EVENT_CONNECT | IOHandler::EVENT_READWRITE | IOHandler::EVENT_CLOSE, do_readwrite)) {
                        printf("accepted[%d]\n", client);
                } else {
                        close(client);
                        perror("ioplexer");
                }
        }
}

int
main(int argc, char** argv)
{
        int sd = accept_socket();

        if(sd < 0) {
                return 1;
        } else {
                IOPlexer ioplexer;
                if(!ioplexer.set(sd, IOHandler::EVENT_ACCEPT, do_accept)) {
                        perror("ioplexer");
                } else {
                        ioplexer.loop(30000);
                        printf("exit dispatch loop after being idle for 30s\n");
                }
                close(sd);
        }
        return 0;
}

/* vim: set ts=8 sts=8 sw=8 ff=unix et: */

