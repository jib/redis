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
#include "limits.h"

#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netdb.h>

void statsdInit(void) {
    // close any old sockets
    if (server.statsd.socket > 0) close(server.statsd.socket);

    // and open the new one
    server.statsd.socket = statsdConnect(server.statsd.host, server.statsd.port);
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

int _statsdSend(int fd, const char *stat) {

    // ******************************
    // Sanity checks
    // ******************************

    // If we didn't get a socket, don't bother trying to send
    if (fd < 1) {
        redisLog(REDIS_WARNING,"Could not get socket for Statsd message %s\n", stat );
        return -1;
    }

    // ******************************
    // Send the packet
    // ******************************

    // Send the stat
    int len  = strlen( stat );
    int sent = write( server.statsd.socket, stat, len );
    //int sent = len;

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
    /* We'll send a handful of stats in one go. To be exact, a total of 4 stats will be sent:
     * command + increment
     * command + duration
     * summary + increment
     * summary + duration
     * It's much more efficient to group these stats together than sending them one by
     * one. In fact, we'll batch up multiple calls worth of stats and send them when
     * the maximum allowed buffer size is reached.
     * Newer versions of statsd support multiple stats in a single packet, if they
     * are separated by a newline.
     */

    // We'll be using this for maths & interpolation.
    const char *prefix  = server.statsd.prefix;
    const char *suffix  = server.statsd.suffix;
    int plen            = strlen(prefix);
    int slen            = strlen(suffix);
    int db              = c->db->id;
    const char *cmd     = c->cmd->name;

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

    /* The total length will be the (prefix + suffix + length of the key + value) * 4
     * The increment key is of length 4 (:1|c). The timer key is of max length 14.
     * (:1234567890|ms), averaging 9. We also need room for dbXX.type.$command, for
     * which I'll reserve 30 chars for now each. And then we need a newline.
     */
    char stat[ (plen + slen + 40)*4 ];

    /* Build the 4 stats as a string - this may be faster (if more cumbersome) using
     * strncat/strncpy */
    snprintf( stat, sizeof(stat),
                "%sdb%d.cmd.%s%s:1|c\n"             // increment command
                "%sdb%d.cmd.%s%s:%lld|ms\n"         // time command
                "%sdb%d.type.%s%s:1|c\n"            // incrememnt type of command
                "%sdb%d.type.%s%s:%lld|ms\n",       // time type of command
                prefix, db, cmd, suffix,            // increment command
                prefix, db, cmd, suffix, duration,  // time command
                prefix, db, type, suffix,           // incrememnt type of command
                prefix, db, type, suffix, duration  // time type of command
            );

    int buf_len     = strlen(server.statsd.buffer);
    int stat_len    = strlen(stat);

    int rv = 0;
    // Time to flush the buffer - there's no room for more stats.
    if (buf_len + stat_len > server.statsd.max_buffer_size) {
        rv = _statsdSend(server.statsd.socket, server.statsd.buffer);
        server.statsd.buffer[0] = '\0';
    }

    // Add this group of stats to the buffer.
    strncat(server.statsd.buffer, stat, stat_len+1);

    return rv;
}


//     redisLog(REDIS_WARNING,"Cur buffer pos: %s\n", server.statsd.cur_buffer_pos);
//
//     server.statsd.cur_buffer_pos = mempcpy(server.statsd.cur_buffer_pos, stat, stat_len);
//
//     redisLog(REDIS_WARNING,"New buffer pos: %s\n", server.statsd.cur_buffer_pos);

//     char stat[] =   "dev.redis.db0.cmd.TEST.vagrant-jib:1|c\n"
//                     "dev.redis.db0.cmd.TEST.vagrant-jib:100|ms\n"
//                     "dev.redis.db0.type.TEST.vagrant-jib:1|c\n"
//                     "dev.redis.db0.type.TEST.vagrant-jib:100|ms\n";



//     redisLog(REDIS_WARNING,"==============");
//     //fprintf(stderr, "New stat:\n%s", stat);
//
//     //redisLog(REDIS_WARNING,"sizeof buffer: %d", sizeof(server.statsd.buffer));
//     redisLog(REDIS_WARNING,"Current buffer size: %d\n", strlen(server.statsd.buffer));
//     redisLog(REDIS_WARNING,"Max char size: %d\n", CHAR_MAX);
//
//     redisLog(REDIS_WARNING,"Stat size: %d\nTotal: %d\nMax: %d",
//         strlen(stat), (strlen(server.statsd.buffer)+strlen(stat)), server.statsd.max_buffer_size);
//
//    // Now, does this still fit in the buffer, or should it be flushed first?
//     int rv = 0;
//     if ((strlen(server.statsd.buffer) + strlen(stat)) >= server.statsd.max_buffer_size) {
//         //redisLog(REDIS_WARNING,"Flushing buffer:\n%s\n\n", server.statsd.buffer);
//         //fprintf(stderr,"Flushing buffer:\n%s\n\n", server.statsd.buffer);
//
//         // Send it, flush it regardless.
//         //rv = _statsdSend(server.statsd.socket, server.statsd.buffer);
//         server.statsd.buffer[0] = '\0';
//
//         redisLog(REDIS_WARNING,"Post flush buffer size: %d\n", strlen(server.statsd.buffer));
//     }
//
//
//     char app[] = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789\n";
//     char new_buf[ strlen(server.statsd.buffer) + strlen(app) + 1 ];
//
//     // Append to the buffer.
//     strncat(server.statsd.buffer, stat, (strlen(stat) + 1));
//
//     redisLog(REDIS_WARNING,"Pre-copy buffer from struct:\n%s\n\n", server.statsd.buffer);
//
//     strncpy( new_buf, server.statsd.buffer, strlen(server.statsd.buffer) );
//
//     strncat( new_buf, app, (strlen(app) + 1));
//
//     server.statsd.buffer = new_buf;
//
//     redisLog(REDIS_WARNING,"New buffer size: %d\n", strlen(server.statsd.buffer));
//
//     redisLog(REDIS_WARNING,"Current buffer from char:\n%s\n\n", new_buf);
//     redisLog(REDIS_WARNING,"Current buffer from struct:\n%s\n\n", server.statsd.buffer);

//    return rv;

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
