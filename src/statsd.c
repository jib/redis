/*
 * Copyright (c) 2013, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2013, Jos Boumans <jos at dwim dot org>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Create a new slowlog entry.
 * Incrementing the ref count of all the objects retained is up to
 * this function. */

#include "redis.h"
#include "statsd.h"

#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>


void statsdInit(void) {
    // close any old sockets
    if (server.statsd_socket > 0) close(server.statsd_socket);

    // and open the new one
    server.statsd_socket = statsdConnect(server.statsd_host, server.statsd_port);
}

/* Connects to a statsd UDP server on host:port, returns the FD for the socket */
int statsdConnect(const char *host, int port) {
    /* Make sure we have a socket to operate on - this won't be initialized on
     * either the very first call, or if someone changed the configuration.
     */

    /* We get the port as an int, but getaddrinfo wants a string for the port.
     * 5 chars for the port (65536 is highest) + null byte
     */
    char port_s[6];
    if (snprintf(port_s, sizeof(port_s), "%d", port) == -1) {
        redisLog(REDIS_WARNING,"Invalid statsd port: %d",port);
        return -1;
    }

    // Grab 2 structs for the connection
    struct addrinfo *statsd;
    statsd = zmalloc(sizeof(struct addrinfo));

    struct addrinfo *hints;
    hints = zmalloc(sizeof(struct addrinfo));

    // what type of socket is the statsd endpoint?
    hints->ai_family   = AF_INET;
    hints->ai_socktype = SOCK_DGRAM;
    hints->ai_protocol = IPPROTO_UDP;
    hints->ai_flags    = 0;

    // using getaddrinfo lets us use a hostname, rather than an
    // ip address.
    int err = getaddrinfo(host, port_s, hints, &statsd);
    if (err != 0) {
        redisLog(REDIS_WARNING,"getaddrinfo on %s:%s failed: %s",
                 host, port, gai_strerror(err));
        return REDIS_ERR;
    }

    // ******************************
    // Store the open connection
    // ******************************

    // getaddrinfo() may return more than one address structure
    // but since this is UDP, we can't verify the connection
    // anyway, so we will just use the first one
    int sock = socket(statsd->ai_family, statsd->ai_socktype, statsd->ai_protocol);

    if (sock == -1) redisLog(REDIS_WARNING,"Could not connect to Statsd %s:%s",host,port_s);

    return sock;
}

int statsdSend(redisClient *c) {
    //fprintf( stderr, "Redis socket: %d\n", server.statsd_socket );



    return 0;
}
