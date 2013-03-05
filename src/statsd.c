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
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>

#define STATSD_BUF_SIZE 500

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

    if (sock == -1) {
        redisLog(REDIS_WARNING,"Could not connect to Statsd %s:%s",host,port_s);
        return -1;
    }

    // connection failed.. for some reason...
    if( connect( sock, statsd->ai_addr, statsd->ai_addrlen ) == -1 ) {
        redisLog(REDIS_WARNING,"Statsd socket connection failed\n" );
        close( sock );
        return -1;
    }

    // now that we have an outgoing socket, we don't need this anymore
    // This segfaults - why?
    //freeaddrinfo(statsd);
    //freeaddrinfo(hints);

    return sock;
}

int _statsdSend(int fd, const char *key, const char *val) {

    // Enough room for the key/val plus prefix/suffix plus newline plus a null byte.
    char stat[ strlen(key) + strlen(val) + 1 ];

    strncpy( stat, key, strlen(key) + 1 );
    strncat( stat, val, strlen(val) + 1 );

    // Newer versions of statsd allow multiple metrics in a single packet, delimited
    // by newlines. That unfortunately means that if we end our message with a new
    // line, statsd will interpret this as an empty second metric and log a 'bad line'.
    // This is true in at least version 0.5.0 and to avoid that, we don't send the
    // newline. Makes debugging using nc -klu 8125 a bit more tricky, but works with
    // modern statsds.
    //strncat( stat, "\n",        1                       );

    // ******************************
    // Sanity checks
    // ******************************

    int len = strlen( stat );

    // +1 for the null byte
    if (len + 1 >= STATSD_BUF_SIZE) {
        redisLog(REDIS_WARNING,"Statsd message length %d > max length %d - ignoring",
                    len, STATSD_BUF_SIZE);
        return -1;
    }

    // If we didn't get a socket, don't bother trying to send
    if (fd < 1) {
        redisLog(REDIS_WARNING,"Could not get socket for Statsd message %s\n", stat );
        return -1;
    }

    // ******************************
    // Send the packet
    // ******************************

    // Send the stat
    int sent = write( server.statsd_socket, stat, len );

    // Should we unset the socket if this happens?
    if (sent != len) {
        redisLog(REDIS_WARNING,
                 "Partial/failed Statsd write for %s on socket %d (len=%d, sent=%d)\n",
                 stat, fd, len, sent);
        return -1;
    }

    return 0;
}

int statsdSend(redisClient *c,long long duration) {


    // enough room to send the stat + prefixes.
    char cmd_key[ strlen(server.statsd_prefix) + strlen(server.statsd_suffix) + 40 ];
    char sum_key[ sizeof(cmd_key) ];

    // Newer versions of statsd support multiple stats in a single packet, if they
    // are separated by a newline. Should we support that here (by default) for
    // speed, or just send multiple packets? For now, stay backwards compat.

    // This is the basic key we're sending for the specific command
    snprintf(cmd_key, sizeof(cmd_key), "%sdb%d.cmd.%s%s",
                server.statsd_prefix, c->db->id, c->cmd->name, server.statsd_suffix);


    // This is the basic key we're sending to summarize the /type/ of commands.
    int flags = c->cmd->flags;
    char type[15];
    strncpy( type,
           (flags & REDIS_CMD_ADMIN     ? "admin"      :
            flags & REDIS_CMD_PUBSUB    ? "pubsub"     :
            flags & REDIS_CMD_WRITE     ? "write"      :
            flags & REDIS_CMD_READONLY  ? "readonly"   :
            "other"),
            sizeof(type));

    snprintf(sum_key, sizeof(sum_key), "%sdb%d.type.%s%s",
                server.statsd_prefix, c->db->id, type, server.statsd_suffix);


    /* A total of 4 stats will be sent:
     * command + increment
     * command + duration
     * summary + increment
     * summary + duration
     */

    // Get the buffer ready. 10 for the maximum lenghth of an int and +5 for metadata
    char time[15];
    snprintf( time, sizeof(time), ":%lld|ms", duration );

    int rv = 0;
    rv += _statsdSend( server.statsd_socket, cmd_key, ":1|c" );
    rv += _statsdSend( server.statsd_socket, cmd_key, time  );
    rv += _statsdSend( server.statsd_socket, sum_key, ":1|c" );
    rv += _statsdSend( server.statsd_socket, sum_key, time  );

    return rv;
}
//
// /* Command flags. Please check the command table defined in the redis.c file
//  * for more information about the meaning of every flag. */
// #define REDIS_CMD_WRITE 1                   /* "w" flag */
// #define REDIS_CMD_READONLY 2                /* "r" flag */
// #define REDIS_CMD_DENYOOM 4                 /* "m" flag */
// #define REDIS_CMD_FORCE_REPLICATION 8       /* "f" flag */
// #define REDIS_CMD_ADMIN 16                  /* "a" flag */
// #define REDIS_CMD_PUBSUB 32                 /* "p" flag */
// #define REDIS_CMD_NOSCRIPT  64              /* "s" flag */
// #define REDIS_CMD_RANDOM 128                /* "R" flag */
// #define REDIS_CMD_SORT_FOR_SCRIPT 256       /* "S" flag */
// #define REDIS_CMD_LOADING 512               /* "l" flag */
// #define REDIS_CMD_STALE 1024                /* "t" flag */
// #define REDIS_CMD_SKIP_MONITOR 2048         /* "M" flag */

/*
 * w: write command (may modify the key space).
 * r: read command  (will never modify the key space).
 * m: may increase memory usage once called. Don't allow if out of memory.
 * a: admin command, like SAVE or SHUTDOWN.
 * p: Pub/Sub related command.
 * f: force replication of this command, regardless of server.dirty.
 * s: command not allowed in scripts.
 * R: random command. Command is not deterministic, that is, the same command
 *    with the same arguments, with the same key space, may have different
 *    results. For instance SPOP and RANDOMKEY are two random commands.
 * S: Sort command output array if called from script, so that the output
 *    is deterministic.
 * l: Allow command while loading the database.
 * t: Allow command while a slave has stale data but is not allowed to
 *    server this data. Normally no command is accepted in this condition
 *    but just a few.
 * M: Do not automatically propagate the command on MONITOR.
*/

// struct redisCommand {
//     char *name;
//     redisCommandProc *proc;
//     int arity;
//     char *sflags; /* Flags as string representation, one char per flag. */
//     int flags;    /* The actual flags, obtained from the 'sflags' field. */
//     /* Use a function to determine keys arguments in a command line. */
//     redisGetKeysProc *getkeys_proc;
//     /* What keys should be loaded in background when calling this command? */
//     int firstkey; /* The first argument that's a key (0 = no keys) */
//     int lastkey;  /* The last argument that's a key */
//     int keystep;  /* The step between first and last key */
//     long long microseconds, calls;
// };
