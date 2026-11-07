/* module-ariacast.c  –  PipeWire sink module for AriaCast (spec v1.1)
 *
 * Design:
 *  - Registers as Audio/Sink so it appears in output device selectors
 *  - Connects to the AriaCast server only when streaming starts
 *    (user routes audio here), disconnects when stopped/idle
 *  - MPRIS2 DBus integration: metadata (xesam:title, xesam:artist,
 *    xesam:album, mpris:artUrl, mpris:length), playback state and
 *    position of the active player; /control commands from the
 *    receiver (play/pause/toggle/stop/next/previous/seek) are
 *    forwarded to that player.
 *
 * Threading rules (this code runs inside the PipeWire daemon):
 *  - on_process() runs on the RT data thread and must never block:
 *    it only uses atomics, pthread_mutex_trylock() and memcpy.
 *  - Sockets are owned by conn_t; a thread using an fd holds a
 *    reference (conn_get/conn_put) and conn_close() waits for all
 *    references to drop before close(), so fd numbers are never reused
 *    under a running read()/send().
 *  - The wake pipe is written once at shutdown and never drained, so
 *    every interruptible sleep returns immediately from then on.
 *
 * SPDX-License-Identifier: MIT
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <ctype.h>
#include <dbus/dbus.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include <pipewire/impl.h>
#include <spa/param/audio/format-utils.h>
#include <spa/pod/builder.h>
#include <spa/utils/result.h>
#include <spa/utils/string.h>

/* ── Module metadata ────────────────────────────────────────────── */
#define MODULE_NAME "libpipewire-module-ariacast"
PW_LOG_TOPIC_STATIC(mod_topic, "mod." MODULE_NAME);
#define PW_LOG_TOPIC_DEFAULT mod_topic

/* ── Config keys ────────────────────────────────────────────────── */
#define KEY_HOST      "ariacast.server.host"
#define KEY_PORT      "ariacast.server.port"
#define KEY_NAME      "ariacast.sink.name"
#define KEY_DESC      "ariacast.sink.description"
#define KEY_DISCOVERY "ariacast.discovery.enabled"
#define KEY_KEEPALIVE "ariacast.pause.keepalive"   /* seconds, 0 = off */

/* ── Protocol constants ─────────────────────────────────────────── */
#define AC_RATE         48000
#define AC_CHANNELS     2
#define AC_FRAME_BYTES  3840        /* 960 × 2ch × 2B = 20 ms      */
#define AC_DISC_PORT    12888
#define AC_STREAM_PORT  12889
#define AC_HS_TIMEOUT   3           /* handshake timeout (seconds)  */
#define AC_DISC_TRIES   5
#define AC_DISC_DELAY   3
#define AC_BO_INIT_MS   1000        /* backoff: 1000 × 2^attempt    */
#define AC_BO_MAX       5           /* cap at 2^5 = 32 s            */
#define AC_IDLE_MS      3000        /* disconnect after 3 s silence */
#define AC_REDISCOVER_EVERY 3       /* re-discover every N failed reconnects */
#define AC_PAUSE_KEEPALIVE_S 600    /* stay connected while paused  */
#define AC_META_EVERY_MS 10000      /* metadata refresh interval    */
#define AC_META_PLAYING_MS 1000     /* ... while playing (position) */
#define MPRIS_POS_POLL_MS  5000     /* re-read Position from player */
#define MPRIS_CMD_MAX      8        /* queued /control commands     */
#define WS_MAX_FRAME    (64*1024)   /* largest frame accepted from the server */
#define META_JSON_MAX   12288       /* fits all fields fully JSON-escaped (×6) */

/* ═══════════════════════════════════════════════════════════════════
 * Small helpers
 * ═══════════════════════════════════════════════════════════════════ */
static const char B64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

static void b64_encode(const uint8_t *in, size_t len, char *out) {
    size_t i, j = 0;
    for (i = 0; i+2 < len; i += 3) {
        out[j++] = B64[in[i]>>2];
        out[j++] = B64[((in[i]&3)<<4)|(in[i+1]>>4)];
        out[j++] = B64[((in[i+1]&0xf)<<2)|(in[i+2]>>6)];
        out[j++] = B64[in[i+2]&0x3f];
    }
    if (len-i==1) { out[j++]=B64[in[i]>>2]; out[j++]=B64[(in[i]&3)<<4]; out[j++]='='; out[j++]='='; }
    else if (len-i==2) { out[j++]=B64[in[i]>>2]; out[j++]=B64[((in[i]&3)<<4)|(in[i+1]>>4)]; out[j++]=B64[(in[i+1]&0xf)<<2]; out[j++]='='; }
    out[j] = 0;
}

/* Minimal SHA-1, only used to verify Sec-WebSocket-Accept (RFC 6455
 * §4.2.2). Input must be shorter than 120 bytes (two 64-byte blocks). */
#define ROL32(x,n) (((x)<<(n))|((x)>>(32-(n))))
static void sha1_small(const uint8_t *in, size_t len, uint8_t out[20]) {
    uint8_t m[128] = {0};
    size_t total = len + 9 <= 64 ? 64 : 128;
    memcpy(m, in, len); m[len] = 0x80;
    uint64_t bits = (uint64_t)len * 8;
    for (int i = 0; i < 8; i++) m[total-1-i] = (uint8_t)(bits >> (8*i));
    uint32_t h[5] = {0x67452301,0xEFCDAB89,0x98BADCFE,0x10325476,0xC3D2E1F0};
    for (size_t off = 0; off < total; off += 64) {
        uint32_t w[80];
        for (int i = 0; i < 16; i++)
            w[i] = (uint32_t)m[off+4*i]<<24 | (uint32_t)m[off+4*i+1]<<16 |
                   (uint32_t)m[off+4*i+2]<<8 | (uint32_t)m[off+4*i+3];
        for (int i = 16; i < 80; i++) w[i] = ROL32(w[i-3]^w[i-8]^w[i-14]^w[i-16], 1);
        uint32_t a=h[0], b=h[1], c=h[2], dd=h[3], e=h[4];
        for (int i = 0; i < 80; i++) {
            uint32_t f, k;
            if      (i < 20) { f = (b & c) | (~b & dd);           k = 0x5A827999; }
            else if (i < 40) { f = b ^ c ^ dd;                    k = 0x6ED9EBA1; }
            else if (i < 60) { f = (b & c) | (b & dd) | (c & dd); k = 0x8F1BBCDC; }
            else             { f = b ^ c ^ dd;                    k = 0xCA62C1D6; }
            uint32_t t = ROL32(a,5) + f + e + k + w[i];
            e = dd; dd = c; c = ROL32(b,30); b = a; a = t;
        }
        h[0]+=a; h[1]+=b; h[2]+=c; h[3]+=dd; h[4]+=e;
    }
    for (int i = 0; i < 5; i++) {
        out[4*i]   = (uint8_t)(h[i]>>24); out[4*i+1] = (uint8_t)(h[i]>>16);
        out[4*i+2] = (uint8_t)(h[i]>>8);  out[4*i+3] = (uint8_t)h[i];
    }
}

/* Fills buf with random bytes. Uses getrandom() so the daemon's global
 * rand() state is left alone; falls back to a clock-seeded xorshift if
 * the entropy pool is not initialised yet (early boot).              */
static void rand_bytes(void *buf, size_t n) {
    uint8_t *p = buf; size_t d = 0;
    while (d < n) {
        ssize_t r = getrandom(p + d, n - d, GRND_NONBLOCK);
        if (r <= 0) break;
        d += (size_t)r;
    }
    if (d < n) {
        struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
        uint64_t x = ((uint64_t)ts.tv_nsec ^ ((uint64_t)ts.tv_sec << 32) ^ (uintptr_t)buf) | 1;
        for (; d < n; d++) { x ^= x<<13; x ^= x>>7; x ^= x<<17; p[d] = (uint8_t)x; }
    }
}

/* snprintf("%s")-style copy that never leaves a truncated UTF-8
 * sequence at the end of dst.                                        */
static void copy_utf8(char *dst, size_t sz, const char *src) {
    if (sz == 0) return;
    size_t n = strlen(src);
    if (n >= sz) {
        n = sz - 1;
        while (n > 0 && ((unsigned char)src[n] & 0xC0) == 0x80) n--;
    }
    memcpy(dst, src, n); dst[n] = 0;
}

/* Escapes a string for use inside a JSON string literal. dst needs
 * 6 × strlen(src) + 1 bytes to never truncate.                       */
static void json_escape(const char *src, char *dst, size_t sz) {
    size_t o = 0;
    for (const unsigned char *p = (const unsigned char *)src; *p; p++) {
        char tmp[8]; const char *s = tmp;
        switch (*p) {
        case '"':  s = "\\\""; break;
        case '\\': s = "\\\\"; break;
        case '\n': s = "\\n";  break;
        case '\r': s = "\\r";  break;
        case '\t': s = "\\t";  break;
        default:
            if (*p < 0x20) snprintf(tmp, sizeof(tmp), "\\u%04x", *p);
            else { tmp[0] = (char)*p; tmp[1] = 0; }
        }
        size_t n = strlen(s);
        if (o + n >= sz) break;
        memcpy(dst + o, s, n); o += n;
    }
    dst[o] = 0;
}

/* Minimal lookup of a string value in a flat JSON object. Handles
 * whitespace and simple backslash escapes; sufficient for the
 * discovery reply.                                                    */
static const char *json_find_value(const char *js, const char *key) {
    char pat[64]; snprintf(pat, sizeof(pat), "\"%s\"", key);
    const char *p = strstr(js, pat); if (!p) return NULL;
    p += strlen(pat);
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    if (*p++ != ':') return NULL;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n') p++;
    return p;
}
static bool json_get_str(const char *js, const char *key, char *out, size_t sz) {
    const char *p = json_find_value(js, key);
    if (!p || *p++ != '"') return false;
    char tmp[512]; size_t o = 0;
    for (; *p && *p != '"'; p++) {
        char c = *p;
        if (c == '\\' && p[1]) { p++; c = *p == 'n' ? '\n' : *p == 't' ? '\t' : *p; }
        if (o + 1 < sizeof(tmp)) tmp[o++] = c;
    }
    if (*p != '"') return false;
    tmp[o] = 0; copy_utf8(out, sz, tmp);
    return true;
}
static bool json_get_long(const char *js, const char *key, long *v) {
    const char *p = json_find_value(js, key); if (!p) return false;
    char *e; errno = 0; long r = strtol(p, &e, 10);
    if (errno || e == p) return false;
    *v = r; return true;
}

static bool parse_port(const char *s, uint16_t *out) {
    char *e; errno = 0; long p = strtol(s, &e, 10);
    if (errno || e == s || *e || p < 1 || p > 65535) return false;
    *out = (uint16_t)p; return true;
}

/* "host:port", with brackets around IPv6 literals. */
static void fmt_hostport(char *out, size_t sz, const char *host, uint16_t port) {
    if (strchr(host, ':')) snprintf(out, sz, "[%s]:%u", host, port);
    else                   snprintf(out, sz, "%s:%u",   host, port);
}

static uint64_t now_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts);
    return (uint64_t)ts.tv_sec*1000ULL + (uint64_t)(ts.tv_nsec/1000000ULL);
}

static void cond_wait_ms(pthread_cond_t *c, pthread_mutex_t *m, long ms) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
    ts.tv_sec  += ms / 1000;
    ts.tv_nsec += (ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) { ts.tv_nsec -= 1000000000L; ts.tv_sec += 1; }
    pthread_cond_timedwait(c, m, &ts);
}

/* ═══════════════════════════════════════════════════════════════════
 * Wakeup pipe + interruptible sleep
 * ═══════════════════════════════════════════════════════════════════ */
static void wake(int wfd) {
    char c=1; ssize_t r=write(wfd,&c,1); (void)r;
}
/* Sleeps up to ms; returns true if woken by wake(). The pipe is never
 * drained, so after shutdown every thread's sleep returns at once.   */
static bool isleep_ms(int rfd, long ms) {
    struct pollfd p = { .fd = rfd, .events = POLLIN };
    return poll(&p, 1, (int)ms) > 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * WebSocket helpers
 * ═══════════════════════════════════════════════════════════════════ */
#define WS_FIN   0x80
#define WS_TEXT  0x01
#define WS_BIN   0x02
#define WS_CLOSE 0x08
#define WS_PING  0x09
#define WS_PONG  0x0A
#define WS_GUID  "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

static void ws_rand_key(char k[25]) {
    uint8_t r[16]; rand_bytes(r, sizeof(r));
    b64_encode(r, 16, k); k[24] = 0;
}
static void ws_accept_key(const char *key, char out[29]) {
    char buf[64]; int n = snprintf(buf, sizeof(buf), "%s" WS_GUID, key);
    uint8_t dig[20]; sha1_small((const uint8_t *)buf, (size_t)n, dig);
    b64_encode(dig, 20, out);
}
static int io_read(int fd, void *b, size_t n) {
    size_t d = 0; uint8_t *p = b;
    while (d < n) {
        ssize_t r = read(fd, p+d, n-d);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        d += (size_t)r;
    }
    return 0;
}
/* MSG_NOSIGNAL: a peer reset must not raise SIGPIPE in the daemon. A
 * short write (send timeout) is reported as failure – the caller then
 * drops the connection instead of leaving a half-sent frame.         */
static int io_write(int fd, const void *b, size_t n) {
    size_t d = 0; const uint8_t *p = b;
    while (d < n) {
        ssize_t r = send(fd, p+d, n-d, MSG_NOSIGNAL);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) return -1;
        d += (size_t)r;
    }
    return 0;
}
static int ws_send_frame(int fd, uint8_t op, const void *pay, size_t len) {
    uint8_t *f = malloc(14 + len); if (!f) return -1;
    size_t hl = 0;
    f[hl++] = WS_FIN | op;
    if      (len <= 125)   f[hl++] = 0x80|(uint8_t)len;
    else if (len <= 65535) { f[hl++]=0x80|126; f[hl++]=(len>>8)&0xff; f[hl++]=len&0xff; }
    else                   { f[hl++]=0x80|127; for(int i=7;i>=0;i--) f[hl++]=((uint64_t)len>>(i*8))&0xff; }
    uint8_t mk[4]; rand_bytes(mk, 4);
    memcpy(f+hl, mk, 4); hl += 4;
    for (size_t i = 0; i < len; i++) f[hl+i] = ((const uint8_t*)pay)[i] ^ mk[i&3];
    int r = io_write(fd, f, hl + len);
    free(f); return r;
}
typedef struct { uint8_t op; uint8_t *data; size_t len; } ws_frame_t;
static int ws_recv_frame(int fd, ws_frame_t *f) {
    f->data = NULL; f->len = 0;
    uint8_t h[2]; if (io_read(fd,h,2)<0) return -1;
    f->op = h[0]&0x0f; bool mk = (h[1]&0x80)!=0;
    uint64_t len = h[1]&0x7f;
    if (len==126){ uint8_t e[2]; if(io_read(fd,e,2)<0)return -1; len=((uint64_t)e[0]<<8)|e[1]; }
    else if (len==127){ uint8_t e[8]; if(io_read(fd,e,8)<0)return -1; len=0; for(int i=0;i<8;i++) len=(len<<8)|e[i]; }
    if (len > WS_MAX_FRAME) { pw_log_warn("ws: frame of %" PRIu64 " bytes rejected", len); return -1; }
    uint8_t mask[4]={0}; if (mk && io_read(fd,mask,4)<0) return -1;
    uint8_t *data = malloc((size_t)len+1); if (!data) return -1;
    if (len>0 && io_read(fd,data,(size_t)len)<0) { free(data); return -1; }
    if (mk) for (uint64_t i=0;i<len;i++) data[i]^=mask[i&3];
    data[len]=0; f->data=data; f->len=(size_t)len; return 0;
}

/* ── TCP + WebSocket upgrade ──────────────────────────────────────── */
static int ws_connect(const char *host, uint16_t port, const char *path, int *out) {
    char ps[8]; snprintf(ps,sizeof(ps),"%u",port);
    struct addrinfo hi={.ai_family=AF_UNSPEC,.ai_socktype=SOCK_STREAM}, *res=NULL;
    if (getaddrinfo(host,ps,&hi,&res)!=0) return -1;
    int fd=-1;
    for (struct addrinfo *ai=res; ai; ai=ai->ai_next) {
        fd=socket(ai->ai_family,ai->ai_socktype|SOCK_CLOEXEC,ai->ai_protocol); if(fd<0) continue;
        int one=1; setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
        struct timeval tv={.tv_sec=AC_HS_TIMEOUT};
        setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
        setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
        if (connect(fd,ai->ai_addr,ai->ai_addrlen)==0) break;
        close(fd); fd=-1;
    }
    freeaddrinfo(res); if (fd<0) return -1;
    char key[25]; ws_rand_key(key);
    char hp[300]; fmt_hostport(hp, sizeof(hp), host, port);
    char req[512];
    int rl = snprintf(req,sizeof(req),
        "GET %s HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\n"
        "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n", path,hp,key);
    if (rl<0 || (size_t)rl>=sizeof(req) || io_write(fd,req,(size_t)rl)<0) { close(fd); return -1; }
    char resp[2048]; int n=0; bool done=false;
    while (n<(int)sizeof(resp)-1) {
        if (read(fd,resp+n,1)<=0) { close(fd); return -1; }
        n++; if (n>=4 && memcmp(resp+n-4,"\r\n\r\n",4)==0) { done=true; break; }
    }
    resp[n]=0;
    if (!done || strncmp(resp,"HTTP/1.1 101",12)!=0) {
        pw_log_warn("%s: upgrade rejected", path); close(fd); return -1;
    }
    char want[29]; ws_accept_key(key, want);
    const char *a = strcasestr(resp, "\r\nSec-WebSocket-Accept:");
    if (a) { a += 23; while (*a==' '||*a=='\t') a++; }
    if (!a || strncmp(a,want,28)!=0) {
        pw_log_warn("%s: bad Sec-WebSocket-Accept", path); close(fd); return -1;
    }
    struct timeval tv={0}; setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    *out=fd; return 0;
}

/* ═══════════════════════════════════════════════════════════════════
 * Ring buffer  (RT process callback → sender thread)
 * ═══════════════════════════════════════════════════════════════════ */
#define RING_CAP 256
typedef struct {
    uint8_t  data[RING_CAP][AC_FRAME_BYTES];
    uint32_t head, tail, count;
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
    bool closing;
} ring_t;

static void ring_init(ring_t *r) {
    pthread_mutex_init(&r->lock,NULL);
    pthread_cond_init(&r->not_empty,NULL);
}
static void ring_close(ring_t *r) {
    pthread_mutex_lock(&r->lock); r->closing=true;
    pthread_cond_broadcast(&r->not_empty);
    pthread_mutex_unlock(&r->lock);
}
static void ring_destroy(ring_t *r) {
    pthread_mutex_destroy(&r->lock);
    pthread_cond_destroy(&r->not_empty);
}
/* Called from the RT process callback: never blocks. Returns false
 * (frame dropped) if the ring is full or the consumer holds the lock. */
static bool ring_push(ring_t *r, const uint8_t *f) {
    if (pthread_mutex_trylock(&r->lock) != 0) return false;
    if (r->closing || r->count==RING_CAP) { pthread_mutex_unlock(&r->lock); return false; }
    memcpy(r->data[r->head],f,AC_FRAME_BYTES);
    r->head=(r->head+1)%RING_CAP; r->count++;
    pthread_cond_signal(&r->not_empty); pthread_mutex_unlock(&r->lock); return true;
}
static bool ring_pop(ring_t *r, uint8_t *f) {
    pthread_mutex_lock(&r->lock);
    while (r->count==0 && !r->closing) pthread_cond_wait(&r->not_empty,&r->lock);
    if (r->count==0) { pthread_mutex_unlock(&r->lock); return false; }
    memcpy(f,r->data[r->tail],AC_FRAME_BYTES);
    r->tail=(r->tail+1)%RING_CAP; r->count--;
    pthread_mutex_unlock(&r->lock); return true;
}
static void ring_flush(ring_t *r) {
    pthread_mutex_lock(&r->lock);
    r->head = r->tail = r->count = 0;
    pthread_mutex_unlock(&r->lock);
}

/* ═══════════════════════════════════════════════════════════════════
 * Per-connection state
 *
 * conn_get() returns the fd with a reference held; conn_put() drops
 * it. conn_close() detaches the fd, shutdown()s it to wake blocked
 * readers/writers, waits until no reference is left and only then
 * close()s it – so the fd number cannot be reused while in use.
 * All sends go through conn_send() so frames never interleave.
 * ═══════════════════════════════════════════════════════════════════ */
typedef struct {
    int             fd;
    unsigned        users;
    pthread_mutex_t lock;
    pthread_cond_t  idle;
    pthread_mutex_t send_lock;
} conn_t;
static void conn_init(conn_t *c) {
    c->fd=-1; c->users=0;
    pthread_mutex_init(&c->lock,NULL);
    pthread_cond_init(&c->idle,NULL);
    pthread_mutex_init(&c->send_lock,NULL);
}
static void conn_destroy(conn_t *c) {
    pthread_mutex_destroy(&c->lock);
    pthread_cond_destroy(&c->idle);
    pthread_mutex_destroy(&c->send_lock);
}
static int conn_get(conn_t *c) {
    pthread_mutex_lock(&c->lock);
    int f=c->fd; if (f>=0) c->users++;
    pthread_mutex_unlock(&c->lock); return f;
}
static void conn_put(conn_t *c) {
    pthread_mutex_lock(&c->lock);
    if (--c->users==0) pthread_cond_broadcast(&c->idle);
    pthread_mutex_unlock(&c->lock);
}
/* fd must have been obtained with conn_get() and still be held. */
static int conn_send(conn_t *c, int fd, uint8_t op, const void *pay, size_t len) {
    pthread_mutex_lock(&c->send_lock);
    int r = ws_send_frame(fd, op, pay, len);
    pthread_mutex_unlock(&c->send_lock);
    return r;
}
/* Must not be called while holding a reference on c. */
static void conn_close_if(conn_t *c, int only_fd, bool graceful) {
    pthread_mutex_lock(&c->lock);
    int f=c->fd;
    if (f<0 || (only_fd>=0 && f!=only_fd)) { pthread_mutex_unlock(&c->lock); return; }
    c->fd=-1;
    pthread_mutex_unlock(&c->lock);

    if (graceful) {
        pthread_mutex_lock(&c->send_lock);
        ws_send_frame(f,WS_CLOSE,NULL,0);
        pthread_mutex_unlock(&c->send_lock);
    }
    shutdown(f,SHUT_RDWR);
    pthread_mutex_lock(&c->lock);
    while (c->users>0) pthread_cond_wait(&c->idle,&c->lock);
    pthread_mutex_unlock(&c->lock);
    close(f);
}
static void conn_close(conn_t *c, bool graceful) { conn_close_if(c, -1, graceful); }
/* Closes fd only if it is still the current one (a newer connection
 * may already have replaced it).                                     */
static void conn_drop(conn_t *c, int fd) { conn_close_if(c, fd, false); }
static void conn_set(conn_t *c, int f) {
    conn_close(c, true);
    pthread_mutex_lock(&c->lock); c->fd=f; pthread_mutex_unlock(&c->lock);
}

/* ═══════════════════════════════════════════════════════════════════
 * Metadata state
 * ═══════════════════════════════════════════════════════════════════ */
/* PlaybackStatus of the active MPRIS player */
enum { MPRIS_STATE_UNKNOWN, MPRIS_STATE_PLAYING, MPRIS_STATE_PAUSED, MPRIS_STATE_STOPPED };

typedef struct {
    char    title[256], artist[256], album[256], artwork[512];
    char    last_trackid[256]; /* mpris:trackid of the last processed update */
    bool    title_placeholder; /* title is the sink name, not from MPRIS     */
    int64_t dur_ms;            /* mpris:length, 0 = unknown                  */
    int64_t pos_ms;            /* player position at pos_at_ms ...           */
    uint64_t pos_at_ms;        /* ... (CLOCK_MONOTONIC); extrapolated while
                                *  playing, see meta_pos_locked()            */
    double  rate;              /* MPRIS Rate (playback speed)                */
    int     state;             /* MPRIS_STATE_*                              */
    bool    playing;           /* connected and streaming to the receiver    */
    bool    playing_reported;  /* receiver acknowledged reported_playing ... */
    bool    reported_playing;  /* ... since the last (re)connect             */
    bool    dirty;             /* thr_meta should POST as soon as possible   */
    pthread_mutex_t lock;
    pthread_cond_t  cond;
} meta_t;

static void meta_init(meta_t *m) {
    m->rate = 1.0;
    pthread_mutex_init(&m->lock,NULL);
    pthread_cond_init(&m->cond,NULL);
}
/* Current position: last reported one plus elapsed time × rate. */
static int64_t meta_pos_locked(const meta_t *m, uint64_t now) {
    int64_t p = m->pos_ms;
    if (m->state == MPRIS_STATE_PLAYING && m->pos_at_ms)
        p += (int64_t)((double)(now - m->pos_at_ms) * m->rate);
    if (p < 0) p = 0;
    if (m->dur_ms > 0 && p > m->dur_ms) p = m->dur_ms;
    return p;
}
static void meta_set_pos_locked(meta_t *m, int64_t pos_ms, uint64_t now) {
    m->pos_ms = pos_ms; m->pos_at_ms = now;
}
/* isPlaying: streaming, unless the player says it is paused/stopped */
static bool meta_is_playing_locked(const meta_t *m) {
    return m->playing && m->state != MPRIS_STATE_PAUSED && m->state != MPRIS_STATE_STOPPED;
}
static void meta_destroy(meta_t *m) {
    pthread_mutex_destroy(&m->lock);
    pthread_cond_destroy(&m->cond);
}
static void meta_mark_dirty(meta_t *m) {
    pthread_mutex_lock(&m->lock);
    m->dirty = true; pthread_cond_signal(&m->cond);
    pthread_mutex_unlock(&m->lock);
}

/* Returns the JSON length, or -1 if it did not fit. isPlaying/is_playing
 * are included only if playing is non-NULL – see post_meta_now().    */
static int meta_to_json(meta_t *m, char *buf, size_t sz, const bool *playing) {
    char t[256*6], ar[256*6], al[256*6], aw[512*6];
    pthread_mutex_lock(&m->lock);
    json_escape(m->title,   t,  sizeof(t));
    json_escape(m->artist,  ar, sizeof(ar));
    json_escape(m->album,   al, sizeof(al));
    json_escape(m->artwork, aw, sizeof(aw));
    int64_t dur = m->dur_ms, pos = meta_pos_locked(m, now_ms());
    pthread_mutex_unlock(&m->lock);
    char pl[48] = "";
    if (playing) {
        const char *p = *playing ? "true" : "false";
        snprintf(pl, sizeof(pl), ",\"isPlaying\":%s,\"is_playing\":%s", p, p);
    }
    int n = snprintf(buf,sz,
        "{\"title\":\"%s\",\"artist\":\"%s\",\"album\":\"%s\","
        "\"artworkUrl\":\"%s\",\"artwork_url\":\"%s\","
        "\"durationMs\":%" PRId64 ",\"duration_ms\":%" PRId64 ","
        "\"positionMs\":%" PRId64 ",\"position_ms\":%" PRId64 "%s}",
        t, ar, al, aw, aw, dur, dur, pos, pos, pl);
    return (n < 0 || (size_t)n >= sz) ? -1 : n;
}

/* ═══════════════════════════════════════════════════════════════════
 * HTTP POST /metadata  (metadata.md)
 * ═══════════════════════════════════════════════════════════════════ */
/* Returns true if the receiver answered with a 2xx status. */
static bool http_post_meta(const char *host, uint16_t port, const char *mj, size_t mlen) {
    static const char pre[] = "{\"data\":", post[] = "}";
    char hp[300]; fmt_hostport(hp, sizeof(hp), host, port);
    char hdr[512];
    int  hl = snprintf(hdr,sizeof(hdr),
        "POST /metadata HTTP/1.0\r\nHost: %s\r\n"
        "Content-Type: application/json\r\nContent-Length: %zu\r\n"
        "Connection: close\r\n\r\n", hp, sizeof(pre)-1 + mlen + sizeof(post)-1);
    if (hl<0 || (size_t)hl>=sizeof(hdr)) return false;
    char ps[8]; snprintf(ps,sizeof(ps),"%u",port);
    struct addrinfo hi={.ai_family=AF_UNSPEC,.ai_socktype=SOCK_STREAM}, *res=NULL;
    int gai = getaddrinfo(host,ps,&hi,&res);
    if (gai!=0) {
        pw_log_warn("/metadata: getaddrinfo(%s) failed: %s", hp, gai_strerror(gai));
        return false;
    }
    int fd=-1;
    for (struct addrinfo *ai=res; ai; ai=ai->ai_next) {
        fd=socket(ai->ai_family,ai->ai_socktype|SOCK_CLOEXEC,ai->ai_protocol); if(fd<0) continue;
        struct timeval tv={.tv_sec=2};
        setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
        setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
        if (connect(fd,ai->ai_addr,ai->ai_addrlen)==0) break;
        close(fd); fd=-1;
    }
    freeaddrinfo(res);
    if (fd<0) {
        pw_log_warn("/metadata: connect to %s failed: %s", hp, strerror(errno));
        return false;
    }
    if (io_write(fd,hdr,(size_t)hl)<0 || io_write(fd,pre,sizeof(pre)-1)<0 ||
        io_write(fd,mj,mlen)<0        || io_write(fd,post,sizeof(post)-1)<0) {
        pw_log_warn("/metadata: write to %s failed: %s", hp, strerror(errno));
        close(fd);
        return false;
    }
    bool ok = false;
    char rb[128]={0}; ssize_t u=read(fd,rb,sizeof(rb)-1);
    if (u>0) {
        rb[u]=0;
        char *eol=strchr(rb,'\r'); if(eol)*eol=0;
        ok = strncmp(rb,"HTTP/",5)==0 && (eol=strchr(rb,' ')) && eol[1]=='2';
        pw_log_debug("/metadata POST %s -> %s", hp, rb);
    } else {
        pw_log_debug("/metadata POST %s -> (no response, %s)", hp, strerror(errno));
    }
    close(fd);
    return ok;
}

/* ═══════════════════════════════════════════════════════════════════
 * Discovery  (discovery.md)
 * ═══════════════════════════════════════════════════════════════════ */
typedef struct { char host[64]; uint16_t port; char name[128]; } ac_srv_t;

/* Returns 0 on success, -1 if nothing was found or wake_r was signalled. */
static int discover(ac_srv_t *srv, int wake_r) {
    for (int i=1; i<=AC_DISC_TRIES; i++) {
        pw_log_info("discovery attempt %d/%d", i, AC_DISC_TRIES);
        int s=socket(AF_INET,SOCK_DGRAM|SOCK_CLOEXEC,IPPROTO_UDP); if(s<0) goto next;
        int one=1;
        setsockopt(s,SOL_SOCKET,SO_BROADCAST,&one,sizeof(one));
        struct sockaddr_in dst={.sin_family=AF_INET,
                                .sin_port=htons(AC_DISC_PORT),
                                .sin_addr.s_addr=INADDR_BROADCAST};
        const char *msg="DISCOVER_AUDIOCAST";
        if (sendto(s,msg,strlen(msg),0,(struct sockaddr*)&dst,sizeof(dst))<0)
            { pw_log_warn("discovery sendto: %s",strerror(errno)); close(s); goto next; }
        struct pollfd pf[2] = {{.fd=s,.events=POLLIN},{.fd=wake_r,.events=POLLIN}};
        int pr = poll(pf, 2, 2000);
        if (pf[1].revents & POLLIN) { close(s); return -1; }
        char buf[512]; struct sockaddr_in from; socklen_t fl=sizeof(from);
        ssize_t n = -1;
        if (pr > 0 && (pf[0].revents & POLLIN))
            n=recvfrom(s,buf,sizeof(buf)-1,MSG_DONTWAIT,(struct sockaddr*)&from,&fl);
        close(s);
        if (n<=0) { pw_log_debug("no response"); goto next; }
        buf[n]=0; pw_log_info("discovery: %s",buf);
        srv->port=AC_STREAM_PORT; srv->host[0]=srv->name[0]=0;
        if (!json_get_str(buf,"ip",srv->host,sizeof(srv->host)) || !srv->host[0])
            inet_ntop(AF_INET,&from.sin_addr,srv->host,sizeof(srv->host));
        long port;
        if (json_get_long(buf,"port",&port) && port>0 && port<=65535) srv->port=(uint16_t)port;
        json_get_str(buf,"server_name",srv->name,sizeof(srv->name));
        pw_log_info("found \"%s\" at %s:%u",srv->name,srv->host,srv->port);
        return 0;
next:
        if (i<AC_DISC_TRIES && isleep_ms(wake_r, AC_DISC_DELAY*1000)) return -1;
    }
    pw_log_warn("no AriaCast server found");
    return -1;
}

/* ═══════════════════════════════════════════════════════════════════
 * AriaCast connection helpers
 * ═══════════════════════════════════════════════════════════════════ */
static int audio_connect(const char *host, uint16_t port) {
    int fd=-1; if (ws_connect(host,port,"/audio",&fd)<0) return -1;
    struct timeval tv={.tv_sec=AC_HS_TIMEOUT};
    setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    ws_frame_t f={0};
    if (ws_recv_frame(fd,&f)<0) { pw_log_warn("/audio: no handshake"); close(fd); return -1; }
    bool ok=(f.op==WS_TEXT)&&(strstr((char*)f.data,"\"READY\"")||strstr((char*)f.data,"\"handshake\""));
    free(f.data);
    if (!ok) { pw_log_warn("/audio: bad handshake"); close(fd); return -1; }
    tv.tv_sec=0; setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    pw_log_debug("/audio handshake OK"); return fd;
}
static int ctrl_connect(const char *host, uint16_t port) {
    int fd=-1; if (ws_connect(host,port,"/control",&fd)<0) return -1;
    struct timeval tv={.tv_sec=1}; setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    ws_frame_t f={0}; if (ws_recv_frame(fd,&f)==0) { pw_log_debug("/control hs: %s",(char*)f.data); free(f.data); }
    tv.tv_sec=0; setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
    return fd;
}
static int stats_connect(const char *host, uint16_t port) {
    int fd=-1; ws_connect(host,port,"/stats",&fd); return fd;
}

/* ═══════════════════════════════════════════════════════════════════
 * Module instance
 * ═══════════════════════════════════════════════════════════════════ */
#define N_THREADS 7

struct data {
    struct pw_impl_module *module;
    struct pw_context     *context;
    struct pw_loop        *loop;      /* main loop */
    struct spa_hook        mod_hook;

    char     host[256];               /* guarded by srv_lock */
    uint16_t port;                    /* guarded by srv_lock */
    pthread_mutex_t srv_lock;
    char     sink_name[128];
    char     sink_desc[256];
    bool     discovery;
    uint32_t pause_keepalive_ms; /* idle limit while the player is paused */
    bool     name_user_set;   /* ariacast.sink.name was configured     */
    bool     desc_user_set;    /* ariacast.sink.description configured  */
    pthread_mutex_t name_lock; /* guards sink_name/sink_desc            */

    /* PipeWire (main loop only) */
    struct pw_stream   *stream;
    struct spa_hook     stream_hook;
    struct pw_core     *core;
    struct spa_hook     core_proxy_hook;
    bool                core_gone; /* set by core_proxy_destroy() before
                                     * pw_context_destroy() frees the
                                     * underlying core/stream objects   */
    /* Signalled by worker threads to create the stream (once the
     * discovered name is known) or to update its description.      */
    struct spa_source  *sink_event;

    /* Worker threads */
    pthread_t   threads[N_THREADS];
    int         n_threads;
    atomic_bool running;

    /* Wakeup pipe – written once at shutdown, see isleep_ms() */
    int  wake_r, wake_w;

    /* Connection state.
     * want_connect is driven by ACTUAL AUDIO ACTIVITY (on_process),
     * not merely by the stream being linked – PipeWire sets
     * STREAMING as soon as the sink is linked, even while silent.
     * state_lock/state_cond only serve to wake thr_connmgr; the RT
     * thread merely try-locks, so waiters also poll with a timeout. */
    atomic_bool connected;       /* true after successful connect  */
    atomic_bool want_connect;    /* true while non-silent audio is flowing */
    pthread_mutex_t state_lock;
    pthread_cond_t  state_cond;

    /* Low 32 bits of CLOCK_MONOTONIC ms of the last non-silent frame
     * (0 = none). Written by the RT process callback.                */
    _Atomic uint32_t last_audio_ms;
    atomic_uint      frames_dropped;

    ring_t  ring;
    meta_t  meta;
    conn_t  audio_conn, ctrl_conn, stats_conn;

    /* /control commands for the active MPRIS player: queued by
     * thr_ctrl, executed by thr_mpris (which owns the DBus
     * connection), woken via the cmd_fd eventfd.                    */
    struct { char action[16]; int64_t pos_ms; } cmds[MPRIS_CMD_MAX];
    int             n_cmds;
    pthread_mutex_t cmd_lock;
    int             cmd_fd;

    /* Frame accumulation buffer for the AriaCast 20ms/3840B protocol
     * frame size (RT thread only). PipeWire's audioconvert negotiates
     * the stream to AC_RATE/AC_CHANNELS/S16LE (see create_stream()),
     * so on_process() always receives audio already at 48kHz - no
     * resampling is done here.                                        */
    uint8_t  accum[AC_FRAME_BYTES];
    size_t   accum_fill;
};

static bool is_running(struct data *d) { return atomic_load(&d->running); }

static uint16_t get_server(struct data *d, char *host, size_t sz) {
    pthread_mutex_lock(&d->srv_lock);
    snprintf(host, sz, "%s", d->host); uint16_t p = d->port;
    pthread_mutex_unlock(&d->srv_lock);
    return p;
}

/* isPlaying is sent only when it differs from what the receiver last
 * accepted (and once after every connect). Receivers act on it, not
 * just display it: Music Assistant restarts playback on true and drops
 * the AriaCast source on false. A periodic refresh repeating a stale
 * "true" while the receiver is pausing would resume playback there.  */
static void post_meta_now(struct data *d) {
    char host[256]; uint16_t port = get_server(d, host, sizeof(host));
    pthread_mutex_lock(&d->meta.lock);
    bool playing = meta_is_playing_locked(&d->meta);
    bool with_playing = !d->meta.playing_reported || d->meta.reported_playing != playing;
    pthread_mutex_unlock(&d->meta.lock);

    char mj[META_JSON_MAX];
    int n = meta_to_json(&d->meta, mj, sizeof(mj), with_playing ? &playing : NULL);
    if (n < 0) { pw_log_warn("/metadata: JSON too large, not sent"); return; }
    if (!http_post_meta(host, port, mj, (size_t)n) || !with_playing) return;

    pthread_mutex_lock(&d->meta.lock);
    d->meta.playing_reported = true;
    d->meta.reported_playing = playing;
    pthread_mutex_unlock(&d->meta.lock);
    pw_log_debug("/metadata: reported isPlaying=%s", playing ? "true" : "false");
}

/* ═══════════════════════════════════════════════════════════════════
 * MPRIS2 integration
 *
 * Only standard MPRIS2 keys are used:
 *   xesam:title    -> title
 *   xesam:artist   -> artist (first element of the array)
 *   xesam:album    -> album
 *   mpris:artUrl   -> artworkUrl
 *   mpris:length   -> durationMs
 *   mpris:trackid  -> used to detect track changes, so album/artwork
 *                     can be cleared when a new track has none of its
 *                     own (no stale cover/album shown), and as the
 *                     target of SetPosition for seeks.
 * Player properties:
 *   PlaybackStatus -> isPlaying
 *   Position/Rate  -> positionMs (extrapolated between updates; the
 *                     player does not signal Position changes, so it is
 *                     re-read every MPRIS_POS_POLL_MS and on Seeked)
 *
 * Only org.mpris.MediaPlayer2.Player is considered, and only the
 * "active" player: the one that most recently reported
 * PlaybackStatus=Playing (or the first one seen, until then). /control
 * commands from the receiver are forwarded to that player.
 *
 * Empty-string values are treated as "field not present", since some
 * players send empty strings for fields they have cleared.
 * ═══════════════════════════════════════════════════════════════════ */
#define MPRIS_IFACE   "org.mpris.MediaPlayer2.Player"
#define MPRIS_PATH    "/org/mpris/MediaPlayer2"
#define DBUS_PROPS    "org.freedesktop.DBus.Properties"
#define MPRIS_NOTRACK "/org/mpris/MediaPlayer2/TrackList/NoTrack"
#define MPRIS_MATCH_PROPS \
    "type='signal',interface='" DBUS_PROPS "'," \
    "member='PropertiesChanged',path='" MPRIS_PATH "',arg0='" MPRIS_IFACE "'"
#define MPRIS_MATCH_SEEKED \
    "type='signal',interface='" MPRIS_IFACE "',member='Seeked',path='" MPRIS_PATH "'"

/* thr_mpris-local state */
typedef struct {
    DBusConnection *c;
    char            active[256]; /* unique bus name of the active player */
} mpris_t;

static bool dbus_iter_get_string(DBusMessageIter *it, char *out, size_t sz) {
    int t = dbus_message_iter_get_arg_type(it);
    if (t == DBUS_TYPE_STRING || t == DBUS_TYPE_OBJECT_PATH) {
        const char *s = NULL; dbus_message_iter_get_basic(it, &s);
        if (s) { copy_utf8(out, sz, s); return true; }
    } else if (t == DBUS_TYPE_ARRAY) {
        DBusMessageIter sub; dbus_message_iter_recurse(it, &sub);
        if (dbus_message_iter_get_arg_type(&sub) == DBUS_TYPE_STRING) {
            const char *s = NULL; dbus_message_iter_get_basic(&sub, &s);
            if (s) { copy_utf8(out, sz, s); return true; }
        }
    }
    return false;
}

/* mpris:length / Position are specified as int64 (µs), but players
 * also send uint64, int32 or double.                                 */
static bool dbus_iter_get_int64(DBusMessageIter *it, int64_t *out) {
    switch (dbus_message_iter_get_arg_type(it)) {
    case DBUS_TYPE_INT64:  { dbus_int64_t v;  dbus_message_iter_get_basic(it,&v); *out=(int64_t)v; return true; }
    case DBUS_TYPE_UINT64: { dbus_uint64_t v; dbus_message_iter_get_basic(it,&v); *out=(int64_t)v; return true; }
    case DBUS_TYPE_INT32:  { dbus_int32_t v;  dbus_message_iter_get_basic(it,&v); *out=v; return true; }
    case DBUS_TYPE_UINT32: { dbus_uint32_t v; dbus_message_iter_get_basic(it,&v); *out=v; return true; }
    case DBUS_TYPE_DOUBLE: { double v;        dbus_message_iter_get_basic(it,&v); *out=(int64_t)v; return true; }
    }
    return false;
}

/* new_player: the update comes from a different player than the last
 * one, so album/artwork/length it doesn't provide must be cleared.   */
static void mpris_process_metadata(struct data *d, DBusMessageIter *dict, bool new_player) {
    char title[256]   = "";
    char artist[256]  = "";
    char album[256]   = "";
    char artwork[512] = "";
    char trackid[256] = "";
    int64_t length_us = 0;
    bool has_title = false, has_artist = false, has_album = false,
         has_artwork = false, has_trackid = false, has_length = false;

    while (dbus_message_iter_get_arg_type(dict) == DBUS_TYPE_DICT_ENTRY) {
        DBusMessageIter entry, var;
        dbus_message_iter_recurse(dict, &entry);
        const char *key = NULL;
        dbus_message_iter_get_basic(&entry, &key);
        dbus_message_iter_next(&entry);
        if (dbus_message_iter_get_arg_type(&entry) != DBUS_TYPE_VARIANT) goto next_key;
        dbus_message_iter_recurse(&entry, &var);
        if      (key && strcmp(key,"xesam:title")==0)   has_title   = dbus_iter_get_string(&var,title,  sizeof(title));
        else if (key && strcmp(key,"xesam:artist")==0)  has_artist  = dbus_iter_get_string(&var,artist, sizeof(artist));
        else if (key && strcmp(key,"xesam:album")==0)   has_album   = dbus_iter_get_string(&var,album,  sizeof(album));
        else if (key && strcmp(key,"mpris:artUrl")==0)  has_artwork = dbus_iter_get_string(&var,artwork,sizeof(artwork));
        else if (key && strcmp(key,"mpris:trackid")==0) has_trackid = dbus_iter_get_string(&var,trackid,sizeof(trackid));
        else if (key && strcmp(key,"mpris:length")==0)  has_length  = dbus_iter_get_int64(&var,&length_us);
next_key:
        dbus_message_iter_next(dict);
    }

    /* Treat empty strings as "not present" */
    if (has_title   && title[0]   == '\0') has_title   = false;
    if (has_artist  && artist[0]  == '\0') has_artist  = false;
    if (has_album   && album[0]   == '\0') has_album   = false;
    if (has_artwork && artwork[0] == '\0') has_artwork = false;
    if (has_length  && length_us  <= 0)    has_length  = false;

    bool changed = false;
    pthread_mutex_lock(&d->meta.lock);

    /* mpris:trackid changes whenever the player switches to a different
     * track/stream. Used to clear album/artwork that don't apply to the
     * new track when the new update doesn't provide its own. Some players
     * keep one trackid for everything (Firefox), so a changed title also
     * counts as a new track.                                           */
    bool track_changed = new_player ||
        (has_trackid && strcmp(d->meta.last_trackid, trackid) != 0) ||
        (has_title && !d->meta.title_placeholder && strcmp(d->meta.title, title) != 0);
    if (has_trackid)
        snprintf(d->meta.last_trackid, sizeof(d->meta.last_trackid), "%s", trackid);
    else if (new_player)
        d->meta.last_trackid[0] = '\0';

    /* A new track starts at 0 unless a Position update says otherwise */
    if (track_changed) meta_set_pos_locked(&d->meta, 0, now_ms());

    if (has_title && (d->meta.title_placeholder || strcmp(d->meta.title, title) != 0)) {
        snprintf(d->meta.title, sizeof(d->meta.title), "%s", title);
        d->meta.title_placeholder = false; changed=true;
    }
    if (has_artist && strcmp(d->meta.artist, artist) != 0)
        { snprintf(d->meta.artist, sizeof(d->meta.artist), "%s", artist); changed=true; }

    if (has_album) {
        if (strcmp(d->meta.album, album) != 0)
            { snprintf(d->meta.album, sizeof(d->meta.album), "%s", album); changed=true; }
    } else if (track_changed && d->meta.album[0]) {
        d->meta.album[0] = '\0'; changed=true;
    }

    if (has_artwork) {
        if (strcmp(d->meta.artwork, artwork) != 0)
            { snprintf(d->meta.artwork, sizeof(d->meta.artwork), "%s", artwork); changed=true; }
    } else if (track_changed && d->meta.artwork[0]) {
        d->meta.artwork[0] = '\0'; changed=true;
    }

    /* Metadata is always the complete map (MPRIS spec), so an update
     * without mpris:length means the length is unknown – e.g. the next
     * video is a livestream while trackid and title stay the same.     */
    int64_t dur_ms = has_length ? length_us / 1000 : 0;
    if (d->meta.dur_ms != dur_ms)
        { d->meta.dur_ms = dur_ms; changed=true; }

    if (changed)
        pw_log_info("MPRIS meta: title='%s' artist='%s' duration=%" PRId64 " ms",
                    d->meta.title, d->meta.artist, d->meta.dur_ms);
    pthread_mutex_unlock(&d->meta.lock);

    /* thr_meta posts it right away – but only while connected */
    if (changed) meta_mark_dirty(&d->meta);
}

static int mpris_parse_status(const char *s) {
    if (strcmp(s, "Playing") == 0) return MPRIS_STATE_PLAYING;
    if (strcmp(s, "Paused")  == 0) return MPRIS_STATE_PAUSED;
    if (strcmp(s, "Stopped") == 0) return MPRIS_STATE_STOPPED;
    return MPRIS_STATE_UNKNOWN;
}

/* Finds a string property in an a{sv} iterator (passed by value, so
 * the caller's iterator is not advanced).                            */
static bool mpris_find_string_prop(DBusMessageIter props, const char *name, char *out, size_t sz) {
    while (dbus_message_iter_get_arg_type(&props) == DBUS_TYPE_DICT_ENTRY) {
        DBusMessageIter entry, var;
        dbus_message_iter_recurse(&props, &entry);
        const char *key = NULL; dbus_message_iter_get_basic(&entry, &key);
        dbus_message_iter_next(&entry);
        if (key && strcmp(key, name) == 0 &&
            dbus_message_iter_get_arg_type(&entry) == DBUS_TYPE_VARIANT) {
            dbus_message_iter_recurse(&entry, &var);
            return dbus_iter_get_string(&var, out, sz);
        }
        dbus_message_iter_next(&props);
    }
    return false;
}

/* Applies an a{sv} of Player properties (from PropertiesChanged or
 * GetAll): Metadata, PlaybackStatus, Position, Rate.                 */
static void mpris_apply_props(struct data *d, DBusMessageIter *props, bool new_player) {
    DBusMessageIter meta_dict; bool has_meta = false, has_pos = false, has_rate = false;
    char status[32] = "";
    int64_t pos_us = 0; double rate = 1.0;

    while (dbus_message_iter_get_arg_type(props) == DBUS_TYPE_DICT_ENTRY) {
        DBusMessageIter entry, var;
        dbus_message_iter_recurse(props, &entry);
        const char *key=NULL; dbus_message_iter_get_basic(&entry,&key);
        dbus_message_iter_next(&entry);
        if (key && dbus_message_iter_get_arg_type(&entry)==DBUS_TYPE_VARIANT) {
            dbus_message_iter_recurse(&entry,&var);
            if (strcmp(key,"Metadata")==0 && dbus_message_iter_get_arg_type(&var)==DBUS_TYPE_ARRAY) {
                dbus_message_iter_recurse(&var,&meta_dict); has_meta = true;
            } else if (strcmp(key,"PlaybackStatus")==0) {
                dbus_iter_get_string(&var, status, sizeof(status));
            } else if (strcmp(key,"Position")==0) {
                has_pos = dbus_iter_get_int64(&var, &pos_us);
            } else if (strcmp(key,"Rate")==0 && dbus_message_iter_get_arg_type(&var)==DBUS_TYPE_DOUBLE) {
                dbus_message_iter_get_basic(&var, &rate); has_rate = rate > 0.0;
            }
        }
        dbus_message_iter_next(props);
    }

    if (has_meta) mpris_process_metadata(d, &meta_dict, new_player);

    bool changed = false;
    uint64_t now = now_ms();
    pthread_mutex_lock(&d->meta.lock);
    if (has_pos) {
        meta_set_pos_locked(&d->meta, pos_us / 1000, now);
        changed = true;
    }
    /* Rate or state changes: rebase the extrapolated position first */
    if (has_rate && rate != d->meta.rate) {
        meta_set_pos_locked(&d->meta, meta_pos_locked(&d->meta, now), now);
        d->meta.rate = rate;
    }
    bool resumed = false;
    if (status[0]) {
        int st = mpris_parse_status(status);
        if (st != d->meta.state) {
            meta_set_pos_locked(&d->meta, meta_pos_locked(&d->meta, now), now);
            d->meta.state = st;
            resumed = st == MPRIS_STATE_PLAYING;
            pw_log_info("MPRIS: player is %s", status);
            changed = true;
        }
    }
    pthread_mutex_unlock(&d->meta.lock);
    /* Resuming ends the pause keep-alive, but the player's audio reaches
     * the sink only a moment later. Restart the idle timer so the
     * watchdog doesn't disconnect in between – a reconnect makes
     * Music Assistant drop the AriaCast source.                       */
    if (resumed && atomic_load(&d->want_connect)) {
        uint32_t t = (uint32_t)now;
        atomic_store(&d->last_audio_ms, t ? t : 1);
    }
    if (changed) meta_mark_dirty(&d->meta);
}

/* Forget player-provided playback state (no active player any more). */
static void mpris_forget_player(struct data *d, mpris_t *mp) {
    mp->active[0] = '\0';
    pthread_mutex_lock(&d->meta.lock);
    d->meta.state = MPRIS_STATE_UNKNOWN;
    d->meta.rate  = 1.0;
    d->meta.dur_ms = 0;
    meta_set_pos_locked(&d->meta, 0, 0);
    pthread_mutex_unlock(&d->meta.lock);
    meta_mark_dirty(&d->meta);
}

/* Drops the active player if a call failed because it is gone. */
static void mpris_check_gone(struct data *d, mpris_t *mp, DBusError *err) {
    if (dbus_error_has_name(err, DBUS_ERROR_SERVICE_UNKNOWN) ||
        dbus_error_has_name(err, DBUS_ERROR_NAME_HAS_NO_OWNER)) {
        pw_log_info("MPRIS: player %s is gone", mp->active);
        mpris_forget_player(d, mp);
    }
}

/* Blocking Properties call on the active player (500 ms timeout). */
static DBusMessage *mpris_props_call(struct data *d, mpris_t *mp, const char *method, const char *prop) {
    DBusMessage *m = dbus_message_new_method_call(mp->active, MPRIS_PATH, DBUS_PROPS, method);
    if (!m) return NULL;
    const char *iface = MPRIS_IFACE;
    if (prop) dbus_message_append_args(m, DBUS_TYPE_STRING, &iface, DBUS_TYPE_STRING, &prop, DBUS_TYPE_INVALID);
    else      dbus_message_append_args(m, DBUS_TYPE_STRING, &iface, DBUS_TYPE_INVALID);
    DBusError err; dbus_error_init(&err);
    DBusMessage *r = dbus_connection_send_with_reply_and_block(mp->c, m, 500, &err);
    dbus_message_unref(m);
    if (!r) {
        pw_log_debug("MPRIS: %s(%s) on %s failed: %s", method, prop ? prop : "",
                     mp->active, err.message ? err.message : "?");
        mpris_check_gone(d, mp, &err);
        dbus_error_free(&err);
    }
    return r;
}

/* Reads the full state of a player that just became active. Returns
 * false if the call failed.                                          */
static bool mpris_get_all(struct data *d, mpris_t *mp) {
    DBusMessage *r = mpris_props_call(d, mp, "GetAll", NULL);
    if (!r) return false;
    DBusMessageIter it, dict;
    if (dbus_message_iter_init(r, &it) && dbus_message_iter_get_arg_type(&it) == DBUS_TYPE_ARRAY) {
        dbus_message_iter_recurse(&it, &dict);
        mpris_apply_props(d, &dict, true);
    }
    dbus_message_unref(r);
    return true;
}

/* Players don't signal Position changes; re-read it periodically to
 * correct drift of the extrapolated value.                           */
static void mpris_poll_position(struct data *d, mpris_t *mp) {
    if (!mp->active[0]) return;
    pthread_mutex_lock(&d->meta.lock);
    bool playing = d->meta.state == MPRIS_STATE_PLAYING;
    pthread_mutex_unlock(&d->meta.lock);
    if (!playing) return;

    DBusMessage *r = mpris_props_call(d, mp, "Get", "Position");
    if (!r) return;
    DBusMessageIter it, var; int64_t pos_us;
    if (dbus_message_iter_init(r, &it) && dbus_message_iter_get_arg_type(&it) == DBUS_TYPE_VARIANT) {
        dbus_message_iter_recurse(&it, &var);
        if (dbus_iter_get_int64(&var, &pos_us)) {
            pthread_mutex_lock(&d->meta.lock);
            meta_set_pos_locked(&d->meta, pos_us / 1000, now_ms());
            pthread_mutex_unlock(&d->meta.lock);
        }
    }
    dbus_message_unref(r);
}

static void mpris_handle_props_changed(struct data *d, mpris_t *mp, DBusMessage *msg) {
    const char *sender = dbus_message_get_sender(msg);
    if (!sender) return;

    DBusMessageIter iter;
    if (!dbus_message_iter_init(msg, &iter))
        return; /* PropertiesChanged with empty body */

    /* arg 0: interface name (already filtered by the match rule) */
    if (dbus_message_iter_get_arg_type(&iter)!=DBUS_TYPE_STRING) return;
    const char *iface=NULL; dbus_message_iter_get_basic(&iter,&iface);
    if (!iface || strcmp(iface,MPRIS_IFACE)!=0) return;
    dbus_message_iter_next(&iter);

    /* arg 1: a{sv} changed properties */
    if (dbus_message_iter_get_arg_type(&iter) != DBUS_TYPE_ARRAY)
        return;
    DBusMessageIter props; dbus_message_iter_recurse(&iter, &props);

    char status[32] = "";
    mpris_find_string_prop(props, "PlaybackStatus", status, sizeof(status));

    bool new_player = false;
    if (strcmp(mp->active, sender) != 0 && (!mp->active[0] || strcmp(status, "Playing") == 0)) {
        pw_log_info("MPRIS: following player %s", sender);
        snprintf(mp->active, sizeof(mp->active), "%s", sender);
        new_player = true;
    }
    if (strcmp(mp->active, sender) != 0) return; /* not the active player */

    /* A new player's signal usually carries only what changed – read
     * its full state instead; fall back to the signal on failure.    */
    if (!new_player || !mpris_get_all(d, mp))
        mpris_apply_props(d, &props, new_player);
}

static void mpris_handle_seeked(struct data *d, mpris_t *mp, DBusMessage *msg) {
    const char *sender = dbus_message_get_sender(msg);
    if (!sender || strcmp(sender, mp->active) != 0) return;
    dbus_int64_t pos_us;
    if (!dbus_message_get_args(msg, NULL, DBUS_TYPE_INT64, &pos_us, DBUS_TYPE_INVALID)) return;
    pthread_mutex_lock(&d->meta.lock);
    meta_set_pos_locked(&d->meta, pos_us / 1000, now_ms());
    pthread_mutex_unlock(&d->meta.lock);
    meta_mark_dirty(&d->meta);
}

/* ── /control → MPRIS ─────────────────────────────────────────────── */

/* Called from thr_ctrl. Drops the command if the queue is full. */
static void mpris_queue_cmd(struct data *d, const char *action, int64_t pos_ms) {
    bool queued = false;
    pthread_mutex_lock(&d->cmd_lock);
    if (d->n_cmds < MPRIS_CMD_MAX) {
        snprintf(d->cmds[d->n_cmds].action, sizeof(d->cmds[0].action), "%s", action);
        d->cmds[d->n_cmds].pos_ms = pos_ms;
        d->n_cmds++; queued = true;
    }
    pthread_mutex_unlock(&d->cmd_lock);
    if (!queued) { pw_log_warn("/control: command queue full, '%s' dropped", action); return; }
    uint64_t one = 1; ssize_t r = write(d->cmd_fd, &one, sizeof(one)); (void)r;
}

/* Fire-and-forget method call on the active player. */
static void mpris_send(mpris_t *mp, const char *method, int first_type, ...) {
    DBusMessage *m = dbus_message_new_method_call(mp->active, MPRIS_PATH, MPRIS_IFACE, method);
    if (!m) return;
    va_list ap; va_start(ap, first_type);
    bool ok = dbus_message_append_args_valist(m, first_type, ap);
    va_end(ap);
    dbus_message_set_no_reply(m, TRUE);
    if (ok && dbus_connection_send(mp->c, m, NULL))
        pw_log_info("/control: %s -> %s", method, mp->active);
    dbus_message_unref(m);
}

static void mpris_exec_cmd(struct data *d, mpris_t *mp, const char *action, int64_t pos_ms) {
    static const struct { const char *action, *method; } map[] = {
        { "play",       "Play"      },
        { "pause",      "Pause"     },
        { "toggle",     "PlayPause" },
        { "play_pause", "PlayPause" },
        { "stop",       "Stop"      },
        { "next",       "Next"      },
        { "previous",   "Previous"  },
    };
    if (!mp->active[0]) {
        pw_log_info("/control: '%s' ignored – no MPRIS player", action);
        return;
    }
    if (strcmp(action, "seek") == 0) {
        char trackid[256];
        uint64_t now = now_ms();
        pthread_mutex_lock(&d->meta.lock);
        snprintf(trackid, sizeof(trackid), "%s", d->meta.last_trackid);
        int64_t cur = meta_pos_locked(&d->meta, now);
        if (d->meta.dur_ms > 0 && pos_ms > d->meta.dur_ms) pos_ms = d->meta.dur_ms;
        pthread_mutex_unlock(&d->meta.lock);

        dbus_int64_t us = (dbus_int64_t)pos_ms * 1000;
        if (trackid[0] && strcmp(trackid, MPRIS_NOTRACK) != 0 && dbus_validate_path(trackid, NULL)) {
            const char *tp = trackid;
            mpris_send(mp, "SetPosition", DBUS_TYPE_OBJECT_PATH, &tp, DBUS_TYPE_INT64, &us, DBUS_TYPE_INVALID);
        } else {
            /* No usable track id: relative Seek from the estimated position */
            dbus_int64_t off = (dbus_int64_t)(pos_ms - cur) * 1000;
            mpris_send(mp, "Seek", DBUS_TYPE_INT64, &off, DBUS_TYPE_INVALID);
        }
        /* Optimistic; the player's Seeked signal confirms or corrects */
        pthread_mutex_lock(&d->meta.lock);
        meta_set_pos_locked(&d->meta, pos_ms, now);
        pthread_mutex_unlock(&d->meta.lock);
        meta_mark_dirty(&d->meta);
        return;
    }
    for (size_t i = 0; i < SPA_N_ELEMENTS(map); i++) {
        if (strcmp(action, map[i].action) == 0) {
            mpris_send(mp, map[i].method, DBUS_TYPE_INVALID);
            return;
        }
    }
    pw_log_debug("/control: unknown action '%s' ignored", action);
}

static void mpris_run_cmds(struct data *d, mpris_t *mp) {
    uint64_t v; ssize_t r = read(d->cmd_fd, &v, sizeof(v)); (void)r; /* reset eventfd */
    struct { char action[16]; int64_t pos_ms; } cmds[MPRIS_CMD_MAX];
    pthread_mutex_lock(&d->cmd_lock);
    int n = d->n_cmds;
    memcpy(cmds, d->cmds, sizeof(cmds[0]) * (size_t)n);
    d->n_cmds = 0;
    pthread_mutex_unlock(&d->cmd_lock);
    for (int i = 0; i < n; i++) mpris_exec_cmd(d, mp, cmds[i].action, cmds[i].pos_ms);
    if (n > 0) dbus_connection_flush(mp->c);
}

/* ── MPRIS thread ─────────────────────────────────────────────────── */
static DBusConnection *mpris_open(void) {
    DBusError err; dbus_error_init(&err);
    /* A private connection: the shared one from dbus_bus_get() would be
     * visible to everything else in the daemon process.              */
    DBusConnection *c = dbus_bus_get_private(DBUS_BUS_SESSION, &err);
    if (!c) {
        pw_log_warn("MPRIS: DBus connect failed: %s",
                    dbus_error_is_set(&err)?err.message:"unknown");
        dbus_error_free(&err);
        return NULL;
    }
    /* libdbus defaults to _exit() on disconnect – fatal for the daemon */
    dbus_connection_set_exit_on_disconnect(c, FALSE);
    dbus_bus_add_match(c, MPRIS_MATCH_PROPS, &err);
    if (!dbus_error_is_set(&err)) dbus_bus_add_match(c, MPRIS_MATCH_SEEKED, &err);
    if (dbus_error_is_set(&err)) {
        pw_log_warn("MPRIS: add_match failed: %s", err.message);
        dbus_error_free(&err);
        dbus_connection_close(c);
        dbus_connection_unref(c);
        return NULL;
    }
    return c;
}

static void *thr_mpris_fn(void *arg) {
    struct data *d = arg;
    mpris_t mp = {0};

    while (is_running(d)) {
        mp.c = mpris_open();
        if (!mp.c) { isleep_ms(d->wake_r, 2000); continue; }
        pw_log_info("MPRIS2 listener started");
        mpris_forget_player(d, &mp);

        int dfd = -1;
        if (!dbus_connection_get_unix_fd(mp.c, &dfd)) dfd = -1;
        uint64_t next_poll = now_ms() + MPRIS_POS_POLL_MS;

        while (is_running(d) && dbus_connection_get_is_connected(mp.c)) {
            dbus_connection_read_write(mp.c, 0);
            DBusMessage *msg;
            while ((msg=dbus_connection_pop_message(mp.c))!=NULL) {
                if (dbus_message_is_signal(msg,DBUS_PROPS,"PropertiesChanged"))
                    mpris_handle_props_changed(d, &mp, msg);
                else if (dbus_message_is_signal(msg,MPRIS_IFACE,"Seeked"))
                    mpris_handle_seeked(d, &mp, msg);
                dbus_message_unref(msg);
            }
            mpris_run_cmds(d, &mp);

            uint64_t now = now_ms();
            if (now >= next_poll) {
                mpris_poll_position(d, &mp);
                next_poll = now + MPRIS_POS_POLL_MS;
            }
            /* Blocking calls above may have queued further messages */
            if (dbus_connection_get_dispatch_status(mp.c) == DBUS_DISPATCH_DATA_REMAINS)
                continue;

            long to = next_poll > now ? (long)(next_poll - now) : 0;
            if (dfd < 0 && to > 100) to = 100;
            struct pollfd pf[3] = {
                { .fd = dfd,       .events = POLLIN },
                { .fd = d->cmd_fd, .events = POLLIN },
                { .fd = d->wake_r, .events = POLLIN },
            };
            poll(pf, 3, (int)to);
        }

        dbus_connection_close(mp.c);
        dbus_connection_unref(mp.c);
        mp.c = NULL;
        if (is_running(d)) {
            pw_log_warn("MPRIS: DBus connection lost – reconnecting");
            isleep_ms(d->wake_r, 1000);
        }
    }
    pw_log_info("MPRIS2 listener stopped"); return NULL;
}

/* ═══════════════════════════════════════════════════════════════════
 * Connection management
 *
 * want_connect is driven by ACTUAL AUDIO ACTIVITY in on_process(),
 * not by the PipeWire stream's link state (PipeWire sets the stream
 * to STREAMING as soon as it is linked into the graph, even while the
 * routed application is silent or before any app routes to it at all –
 * e.g. simply opening pavucontrol can activate the node).
 *
 * thr_connmgr:
 *   - runs discovery once at startup, then has the sink created
 *   - sleeps until want_connect becomes true
 *   - connects with exponential backoff (re-running discovery every
 *     AC_REDISCOVER_EVERY failed attempts, if discovery is enabled)
 *   - while connected, periodically checks both want_connect and the
 *     health of the /audio socket; on socket loss it reconnects with
 *     backoff as long as want_connect stays true
 *   - disconnects once want_connect becomes false (silence ≥ AC_IDLE_MS)
 * ═══════════════════════════════════════════════════════════════════ */

/* Returns true on success. Does not retry – caller handles backoff. */
static bool do_connect(struct data *d) {
    char host[256]; uint16_t port = get_server(d, host, sizeof(host));
    pw_log_info("connecting to AriaCast %s:%u ...", host, port);

    int fd = audio_connect(host, port);
    if (fd < 0) { pw_log_warn("/audio: connect failed"); return false; }
    conn_set(&d->audio_conn, fd);

    int cfd = ctrl_connect(host, port);
    if (cfd >= 0) conn_set(&d->ctrl_conn, cfd);

    int sfd = stats_connect(host, port);
    if (sfd >= 0) conn_set(&d->stats_conn, sfd);

    pthread_mutex_lock(&d->meta.lock);
    d->meta.playing = true;
    d->meta.playing_reported = false; /* new session: report isPlaying again */
    pthread_mutex_unlock(&d->meta.lock);
    atomic_store(&d->connected, true);
    meta_mark_dirty(&d->meta); /* re-send metadata on (re)connect */
    pw_log_info("connected to AriaCast");
    return true;
}

static void do_disconnect(struct data *d) {
    atomic_store(&d->connected, false);
    conn_close(&d->audio_conn, true);
    conn_close(&d->ctrl_conn,  true);
    conn_close(&d->stats_conn, true);
    /* Drop any buffered frames so the next session starts clean */
    ring_flush(&d->ring);
    pthread_mutex_lock(&d->meta.lock);
    d->meta.playing = false;
    d->meta.playing_reported = false;
    pthread_mutex_unlock(&d->meta.lock);
    pw_log_info("disconnected from AriaCast");
}

/* ── Sink naming from the discovered server_name ──────────────────
 * "Wohnzimmer Lautsprecher"  ->  node name  "ariacast-wohnzimmer-lautsprecher"
 *                                description "Wohnzimmer Lautsprecher [AriaCast]"
 * Each value can be overridden independently by explicitly configuring
 * ariacast.sink.name and/or ariacast.sink.description.                */
static void slugify(const char *in, char *out, size_t outsz) {
    size_t o = 0; bool dash = false;
    for (const char *p = in; *p && o + 1 < outsz; p++) {
        unsigned char c = (unsigned char)*p;
        if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) { out[o++] = (char)c; dash = false; }
        else if (c >= 'A' && c <= 'Z') { out[o++] = (char)(c - 'A' + 'a'); dash = false; }
        else if (!dash && o > 0) { out[o++] = '-'; dash = true; }
    }
    while (o > 0 && out[o-1] == '-') o--;
    out[o] = 0;
}

/* Apply a discovered server_name to the sink name/description. */
static void apply_server_name(struct data *d, const char *server_name) {
    if (!server_name || !server_name[0]) return;

    char name[128], desc[256], slug[112];
    slugify(server_name, slug, sizeof(slug));
    if (slug[0]) snprintf(name, sizeof(name), "ariacast-%s", slug);
    else         snprintf(name, sizeof(name), "%s", "AriaCast");
    snprintf(desc, sizeof(desc), "%s [AriaCast]", server_name);

    bool changed = false;
    pthread_mutex_lock(&d->name_lock);
    if (!d->name_user_set && strcmp(d->sink_name, name) != 0) {
        snprintf(d->sink_name, sizeof(d->sink_name), "%s", name);
        changed = true;
    }
    if (!d->desc_user_set && strcmp(d->sink_desc, desc) != 0) {
        snprintf(d->sink_desc, sizeof(d->sink_desc), "%s", desc);
        changed = true;
    }
    if (changed) {
        pthread_mutex_lock(&d->meta.lock);
        if (d->meta.title_placeholder)
            snprintf(d->meta.title, sizeof(d->meta.title), "%s", d->sink_desc);
        pthread_mutex_unlock(&d->meta.lock);
    }
    pthread_mutex_unlock(&d->name_lock);

    if (changed)
        pw_loop_signal_event(d->loop, d->sink_event);
}

/* Re-run UDP discovery and update host/port if a server responds.
 * Only called when ariacast.server.host was NOT explicitly configured. */
static void redo_discovery(struct data *d) {
    if (!d->discovery) return;
    ac_srv_t srv = {0};
    if (discover(&srv, d->wake_r) != 0) return;
    pthread_mutex_lock(&d->srv_lock);
    if (strcmp(d->host,srv.host)!=0 || d->port!=srv.port)
        pw_log_info("discovered server: %s:%u (was %s:%u)",
                    srv.host, srv.port, d->host, d->port);
    snprintf(d->host, sizeof(d->host), "%s", srv.host);
    d->port = srv.port;
    pthread_mutex_unlock(&d->srv_lock);
    apply_server_name(d, srv.name);
}

/* ── Audio sender thread ─────────────────────────────────────────── */
static void *thr_audio_fn(void *arg) {
    struct data *d = arg;
    uint8_t frame[AC_FRAME_BYTES];
    pw_log_info("audio sender thread started");
    while (is_running(d)) {
        if (!ring_pop(&d->ring, frame)) {
            if (!is_running(d)) break;
            isleep_ms(d->wake_r, 50);
            continue;
        }
        int fd = conn_get(&d->audio_conn);
        if (fd < 0) continue; /* not connected yet – discard frame */
        int r = conn_send(&d->audio_conn, fd, WS_BIN, frame, AC_FRAME_BYTES);
        conn_put(&d->audio_conn);
        if (r < 0) {
            pw_log_warn("/audio: send failed");
            conn_drop(&d->audio_conn, fd); /* thr_connmgr will reconnect */
        }
    }
    pw_log_info("audio sender thread stopped"); return NULL;
}

/* ── Idle watchdog: clears want_connect after AC_IDLE_MS of silence ──
 * While the active MPRIS player is paused, the connection is kept for
 * up to pause_keepalive_ms instead, so a "play" from the receiver can
 * still arrive on /control.                                          */
static uint32_t idle_limit_ms(struct data *d) {
    pthread_mutex_lock(&d->meta.lock);
    bool paused = d->meta.state == MPRIS_STATE_PAUSED;
    pthread_mutex_unlock(&d->meta.lock);
    return paused && d->pause_keepalive_ms > AC_IDLE_MS ? d->pause_keepalive_ms : AC_IDLE_MS;
}
static bool audio_idle(struct data *d, uint32_t limit) {
    uint32_t last = atomic_load(&d->last_audio_ms);
    return last != 0 && (uint32_t)now_ms() - last > limit;
}
static void *thr_watchdog_fn(void *arg) {
    struct data *d = arg;
    bool kept_alive = false;
    while (is_running(d)) {
        isleep_ms(d->wake_r, 250);
        if (!is_running(d)) break;

        unsigned dropped = atomic_exchange(&d->frames_dropped, 0);
        if (dropped) pw_log_warn("dropped %u audio frames (send buffer full)", dropped);

        if (!atomic_load(&d->want_connect)) { kept_alive = false; continue; }
        uint32_t limit = idle_limit_ms(d);
        if (!audio_idle(d, limit)) {
            if (!audio_idle(d, AC_IDLE_MS)) kept_alive = false;
            else if (limit > AC_IDLE_MS && !kept_alive) {
                pw_log_info("player paused – keeping connection for up to %u s",
                            limit / 1000);
                kept_alive = true;
            }
            continue;
        }
        atomic_store(&d->want_connect, false);
        /* on_process() may have stored a fresh timestamp just before we
         * cleared the flag without seeing it change – undo in that case. */
        if (!audio_idle(d, limit)) { atomic_store(&d->want_connect, true); continue; }
        kept_alive = false;
        pthread_mutex_lock(&d->state_lock);
        pthread_cond_broadcast(&d->state_cond);
        pthread_mutex_unlock(&d->state_lock);
        pw_log_info("idle for %u ms – disconnecting", limit);
    }
    return NULL;
}

/* {"action":"play|pause|toggle|stop|next|previous"} or
 * {"action":"seek","position_ms":N} – forwarded to the MPRIS player.
 * isPlaying follows from the player's PlaybackStatus afterwards.     */
static void handle_ctrl_action(struct data *d, const char *j) {
    char action[16];
    if (!json_get_str(j,"action",action,sizeof(action))) {
        pw_log_debug("/control: %s", j);
        return;
    }
    for (char *p = action; *p; p++) *p = (char)tolower((unsigned char)*p);
    long pos = 0;
    if (strcmp(action,"seek")==0 &&
        !json_get_long(j,"position_ms",&pos) && !json_get_long(j,"value",&pos)) {
        pw_log_warn("/control: seek without position: %s", j);
        return;
    }
    mpris_queue_cmd(d, action, pos < 0 ? 0 : pos);
}

static void *thr_ctrl_fn(void *arg) {
    struct data *d = arg;
    while (is_running(d)) {
        int fd = conn_get(&d->ctrl_conn);
        if (fd < 0) { isleep_ms(d->wake_r, 500); continue; }
        ws_frame_t f={0};
        int r = ws_recv_frame(fd,&f);
        if (r==0 && f.op==WS_PING) conn_send(&d->ctrl_conn,fd,WS_PONG,f.data,f.len);
        conn_put(&d->ctrl_conn);
        if (r<0 || f.op==WS_CLOSE) { free(f.data); conn_drop(&d->ctrl_conn,fd); continue; }
        if (f.op==WS_TEXT) handle_ctrl_action(d,(char*)f.data);
        free(f.data);
    }
    return NULL;
}

static void *thr_stats_fn(void *arg) {
    struct data *d = arg;
    /* /stats is best-effort – only the Python server pushes stats
     * (one JSON text frame per second). The Go server does not push
     * any; if it accepts the connection anyway, this thread simply
     * blocks in ws_recv_frame() until conn_close() shuts it down.
     * If the upgrade is rejected, stats_conn stays unset.           */
    while (is_running(d)) {
        int fd = conn_get(&d->stats_conn);
        if (fd < 0) { isleep_ms(d->wake_r, 1000); continue; }
        ws_frame_t f={0};
        int r = ws_recv_frame(fd,&f);
        if (r==0 && f.op==WS_PING) conn_send(&d->stats_conn,fd,WS_PONG,f.data,f.len);
        conn_put(&d->stats_conn);
        if (r<0 || f.op==WS_CLOSE) {
            /* best-effort only: wait for the next connect cycle */
            free(f.data);
            conn_drop(&d->stats_conn,fd);
            pw_log_debug("/stats disconnected (optional endpoint)");
            continue;
        }
        if (f.op==WS_TEXT) pw_log_debug("stats: %s",(char*)f.data);
        free(f.data);
    }
    return NULL;
}

/* Posts metadata immediately when marked dirty, every
 * AC_META_PLAYING_MS while playing (so the receiver's position stays
 * current) and every AC_META_EVERY_MS otherwise – only while connected. */
static void *thr_meta_fn(void *arg) {
    struct data *d = arg;
    while (is_running(d)) {
        pthread_mutex_lock(&d->meta.lock);
        if (!d->meta.dirty && is_running(d))
            cond_wait_ms(&d->meta.cond, &d->meta.lock,
                         meta_is_playing_locked(&d->meta) ? AC_META_PLAYING_MS : AC_META_EVERY_MS);
        d->meta.dirty = false;
        pthread_mutex_unlock(&d->meta.lock);
        if (is_running(d) && atomic_load(&d->connected))
            post_meta_now(d);
    }
    return NULL;
}

/* ── Connection manager thread ────────────────────────────────────── */
static bool wants(struct data *d) { return atomic_load(&d->want_connect); }

static void *thr_connmgr_fn(void *arg) {
    struct data *d = arg;
    pw_log_info("connection manager started");

    /* Run discovery up-front so the sink is created with the server's
     * "server_name" already applied (see create_stream()).          */
    if (d->discovery) {
        redo_discovery(d);
        if (is_running(d)) pw_loop_signal_event(d->loop, d->sink_event);
    }

    while (is_running(d)) {
        /* ── Wait until audio activity requests a connection ─────── */
        pthread_mutex_lock(&d->state_lock);
        while (is_running(d) && !wants(d))
            cond_wait_ms(&d->state_cond, &d->state_lock, 250);
        pthread_mutex_unlock(&d->state_lock);
        if (!is_running(d)) break;

        /* ── Connect with exponential backoff ─────────────────────
         * Re-check want_connect between attempts so a brief audio
         * blip doesn't cause us to keep retrying after the user
         * has already moved on.                                     */
        int attempt = 0;
        bool connected = false;
        while (is_running(d) && wants(d)) {
            connected = do_connect(d);
            if (connected) break;

            attempt++;
            if (d->discovery && (attempt % AC_REDISCOVER_EVERY) == 0) {
                pw_log_info("retrying discovery after %d failed attempts", attempt);
                redo_discovery(d);
            }
            int steps = attempt < AC_BO_MAX ? attempt : AC_BO_MAX;
            long ms = AC_BO_INIT_MS * (1L << steps);
            pw_log_info("reconnect attempt %d – waiting %ld ms", attempt, ms);
            isleep_ms(d->wake_r, ms);
        }
        if (!connected) continue; /* stopping, or want_connect went false */

        /* ── Connected: monitor health + want_connect ─────────────── */
        while (is_running(d)) {
            pthread_mutex_lock(&d->state_lock);
            if (is_running(d) && wants(d))
                cond_wait_ms(&d->state_cond, &d->state_lock, 500);
            pthread_mutex_unlock(&d->state_lock);

            if (!is_running(d) || !wants(d)) break;
            if (conn_get(&d->audio_conn) < 0) {
                pw_log_warn("/audio connection lost while active – reconnecting");
                break;
            }
            conn_put(&d->audio_conn);
        }

        /* Disconnect when idle or stopping. If /audio dropped while
         * want_connect is still true, the loop top re-enters the
         * backoff-connect block (do_connect replaces all sockets).   */
        if (!is_running(d) || !wants(d))
            do_disconnect(d);
    }
    pw_log_info("connection manager stopped"); return NULL;
}

/* ═══════════════════════════════════════════════════════════════════
 * PipeWire stream callbacks
 * ═══════════════════════════════════════════════════════════════════ */
static void on_param_changed(void *arg,uint32_t id,const struct spa_pod *p){
    struct data *d=arg;
    if(id!=SPA_PARAM_Format||!p) return;
    struct spa_audio_info_raw info;
    if(spa_format_audio_raw_parse(p,&info)<0) return;
    if (info.rate != AC_RATE)
        pw_log_warn("negotiated rate %u Hz != expected %u Hz – "
                    "audio will sound wrong (audioconvert should "
                    "prevent this)", info.rate, AC_RATE);
    pw_log_info("format: %u Hz %u ch",info.rate,info.channels);
    (void)d;
}

/* Splits the incoming S16LE/48kHz/2ch buffer into AC_FRAME_BYTES (3840 B,
 * 20 ms) chunks and pushes each complete chunk to the ring buffer. Input
 * is consumed verbatim - no resampling needed (see accum_fill comment
 * on struct data). RT thread: never blocks, drops frames instead.      */
static void push_audio(struct data *d, const uint8_t *pcm, uint32_t nb){
    size_t off = 0;
    while (off < nb) {
        size_t need = AC_FRAME_BYTES - d->accum_fill;
        size_t take = (nb - off) < need ? (nb - off) : need;
        memcpy(d->accum + d->accum_fill, pcm + off, take);
        d->accum_fill += take;
        off += take;
        if (d->accum_fill == AC_FRAME_BYTES) {
            if (!ring_push(&d->ring, d->accum))
                atomic_fetch_add(&d->frames_dropped, 1);
            d->accum_fill = 0;
        }
    }
}

/* Returns true if the S16LE buffer contains at least one non-zero sample.
 * Cheap linear scan – buffers are small (typically 1024 frames × 4 B). */
static bool buf_is_audible(const uint8_t *pcm, uint32_t nbytes) {
    const int16_t *s = (const int16_t *)pcm;
    uint32_t n = nbytes / 2;
    for (uint32_t i = 0; i < n; i++)
        if (s[i] != 0) return true;
    return false;
}

/* RT thread: no blocking locks, no allocation, no logging. */
static void on_process(void *arg){
    struct data *d=arg;
    struct pw_buffer *b=pw_stream_dequeue_buffer(d->stream); if(!b) return;
    struct spa_data *sd=&b->buffer->datas[0];
    uint32_t off  = SPA_MIN(sd->chunk->offset, sd->maxsize);
    uint32_t size = SPA_MIN(sd->chunk->size, sd->maxsize - off);

    if (sd->data && size>0) {
        const uint8_t *pcm = SPA_PTROFF(sd->data, off, const uint8_t);
        if (buf_is_audible(pcm, size)) {
            uint32_t t = (uint32_t)now_ms();
            atomic_store(&d->last_audio_ms, t ? t : 1);
            if (!atomic_exchange(&d->want_connect, true) &&
                pthread_mutex_trylock(&d->state_lock) == 0) {
                pthread_cond_broadcast(&d->state_cond);
                pthread_mutex_unlock(&d->state_lock);
            }
        }
        if (atomic_load(&d->connected))
            push_audio(d, pcm, size);
        else
            d->accum_fill = 0; /* don't carry a partial frame into the next session */
    }
    pw_stream_queue_buffer(d->stream,b);
}

static void on_state_changed(void *arg,
    enum pw_stream_state old, enum pw_stream_state state, const char *err)
{
    struct data *d=arg;
    pw_log_info("stream: %s → %s%s%s%s",
        pw_stream_state_as_string(old),pw_stream_state_as_string(state),
        err?" (":"",err?err:"",err?")":"");

    /* When the stream is fully unlinked from the graph, disconnect
     * immediately regardless of the idle timer.  STREAMING/PAUSED
     * transitions while linked are NOT used to gate the connection –
     * that is driven purely by audio activity in on_process().      */
    if (state==PW_STREAM_STATE_UNCONNECTED ||
        state==PW_STREAM_STATE_ERROR) {
        atomic_store(&d->want_connect, false);
        atomic_store(&d->last_audio_ms, 0);
        pthread_mutex_lock(&d->state_lock);
        pthread_cond_broadcast(&d->state_cond);
        pthread_mutex_unlock(&d->state_lock);
    }
}

static const struct pw_stream_events stream_evts={
    PW_VERSION_STREAM_EVENTS,
    .param_changed=on_param_changed,
    .process=on_process,
    .state_changed=on_state_changed,
};

/* Main loop only. */
static int create_stream(struct data *d) {
    char name[128], desc[256], node[160];
    pthread_mutex_lock(&d->name_lock);
    snprintf(name, sizeof(name), "%s", d->sink_name);
    snprintf(desc, sizeof(desc), "%s", d->sink_desc);
    pthread_mutex_unlock(&d->name_lock);
    slugify(name, node, sizeof(node));
    if (!node[0]) snprintf(node, sizeof(node), "ariacast-sink");

    struct pw_properties *sp = pw_properties_new(
        PW_KEY_DEVICE_ICON_NAME, "audio-speakers",
        PW_KEY_MEDIA_TYPE,       "Audio",
        PW_KEY_MEDIA_CATEGORY,   "Capture",
        PW_KEY_MEDIA_CLASS,      "Audio/Sink",
        PW_KEY_MEDIA_ROLE,       "Music",
        PW_KEY_MEDIA_NAME,       desc,
        PW_KEY_NODE_NAME,        node,
        PW_KEY_NODE_NICK,        desc,
        PW_KEY_NODE_DESCRIPTION, desc,
        PW_KEY_NODE_VIRTUAL,     "true",
        NULL);

    d->stream=pw_stream_new(d->core,name,sp);
    if(!d->stream){ int res=-errno; pw_log_error("pw_stream_new failed: %s", spa_strerror(res)); return res; }
    pw_stream_add_listener(d->stream,&d->stream_hook,&stream_evts,d);

    uint8_t buf[1024]; struct spa_pod_builder b=SPA_POD_BUILDER_INIT(buf,sizeof(buf));
    const struct spa_pod *params[1];
    params[0]=spa_format_audio_raw_build(&b,SPA_PARAM_EnumFormat,
        &SPA_AUDIO_INFO_RAW_INIT(.format=SPA_AUDIO_FORMAT_S16_LE,
                                  .channels=AC_CHANNELS,.rate=AC_RATE,
                                  .position={SPA_AUDIO_CHANNEL_FL,SPA_AUDIO_CHANNEL_FR}));
    int res = pw_stream_connect(d->stream,PW_DIRECTION_INPUT,PW_ID_ANY,
        PW_STREAM_FLAG_MAP_BUFFERS|PW_STREAM_FLAG_RT_PROCESS,
        params,1);
    if (res < 0) {
        pw_log_error("pw_stream_connect failed: %s", spa_strerror(res));
        spa_hook_remove(&d->stream_hook);
        pw_stream_destroy(d->stream);
        d->stream = NULL;
        return res;
    }
    pw_log_info("sink registered: node=\"%s\" description=\"%s\" (idle until selected)", node, desc);
    return 0;
}

/* Main loop: creates the stream on first signal, afterwards applies a
 * changed description. node.name is deliberately left unchanged once
 * the node exists – session managers key routing and default-sink
 * state on it.                                                       */
static void on_sink_event(void *data, uint64_t count) {
    struct data *d = data;
    (void)count;
    if (d->core_gone) return;
    if (!d->stream) {
        if (create_stream(d) < 0) pw_log_error("could not create the AriaCast sink");
        return;
    }
    char desc[256];
    pthread_mutex_lock(&d->name_lock);
    snprintf(desc, sizeof(desc), "%s", d->sink_desc);
    pthread_mutex_unlock(&d->name_lock);
    struct spa_dict_item items[] = {
        SPA_DICT_ITEM_INIT(PW_KEY_NODE_NICK,        desc),
        SPA_DICT_ITEM_INIT(PW_KEY_NODE_DESCRIPTION, desc),
        SPA_DICT_ITEM_INIT(PW_KEY_MEDIA_NAME,       desc),
    };
    struct spa_dict dict = SPA_DICT_INIT(items, SPA_N_ELEMENTS(items));
    pw_stream_update_properties(d->stream, &dict);
    pw_log_info("sink description updated: \"%s\"", desc);
}

/* ═══════════════════════════════════════════════════════════════════
 * Core proxy destroy listener
 *
 * pw_context_connect_self() returns a pw_core* which is also a
 * pw_proxy*. During full daemon shutdown, pw_context_destroy() tears
 * down internal client connections BEFORE invoking each module's
 * .destroy hook. The proxy is freed right after this event, so our
 * hook is unlinked here (removing it later would write into freed
 * memory) and teardown() skips pw_stream_destroy()/pw_core_disconnect()
 * for the reclaimed objects. On a regular module unload this does not
 * fire and teardown() destroys everything itself.
 * ═══════════════════════════════════════════════════════════════════ */
static void core_proxy_destroy(void *arg) {
    struct data *d = arg;
    spa_hook_remove(&d->core_proxy_hook);
    d->core_gone = true;
    pw_log_debug("core proxy destroyed by daemon – skipping manual stream/core teardown");
}
static const struct pw_proxy_events core_proxy_evts = {
    PW_VERSION_PROXY_EVENTS,
    .destroy = core_proxy_destroy,
};

/* ═══════════════════════════════════════════════════════════════════
 * Teardown (module destroy and failed init) – main loop only
 * ═══════════════════════════════════════════════════════════════════ */
static void teardown(struct data *d) {
    atomic_store(&d->running, false);

    /* Wake everything that might be sleeping or blocked */
    wake(d->wake_w);
    atomic_store(&d->want_connect, false);
    pthread_mutex_lock(&d->state_lock);
    pthread_cond_broadcast(&d->state_cond);
    pthread_mutex_unlock(&d->state_lock);
    pthread_mutex_lock(&d->meta.lock);
    pthread_cond_broadcast(&d->meta.cond);
    pthread_mutex_unlock(&d->meta.lock);
    ring_close(&d->ring);
    /* shutdown() wakes threads blocked in read()/send() on these */
    conn_close(&d->audio_conn, false);
    conn_close(&d->ctrl_conn,  false);
    conn_close(&d->stats_conn, false);

    for (int i = 0; i < d->n_threads; i++)
        pthread_join(d->threads[i], NULL);
    d->n_threads = 0;

    /* A connection made concurrently with the closes above */
    conn_close(&d->audio_conn, false);
    conn_close(&d->ctrl_conn,  false);
    conn_close(&d->stats_conn, false);

    /* No thread can signal it any more */
    if (d->sink_event) pw_loop_destroy_source(d->loop, d->sink_event);

    if (d->stream) {
        spa_hook_remove(&d->stream_hook);
        if (!d->core_gone) pw_stream_destroy(d->stream);
        d->stream = NULL;
    }
    if (d->core && !d->core_gone) {
        spa_hook_remove(&d->core_proxy_hook);
        pw_core_disconnect(d->core);
    }
    d->core = NULL;

    close(d->wake_r); close(d->wake_w);
    if (d->cmd_fd >= 0) close(d->cmd_fd);
    pthread_mutex_destroy(&d->cmd_lock);
    ring_destroy(&d->ring);
    conn_destroy(&d->audio_conn);
    conn_destroy(&d->ctrl_conn);
    conn_destroy(&d->stats_conn);
    pthread_mutex_destroy(&d->state_lock);
    pthread_mutex_destroy(&d->name_lock);
    pthread_mutex_destroy(&d->srv_lock);
    pthread_cond_destroy(&d->state_cond);
    meta_destroy(&d->meta);
    free(d);
}

static void on_module_destroy(void *arg){
    struct data *d=arg;
    pw_log_info("destroying");
    /* d->mod_hook is embedded in *d and must be unlinked before
     * teardown() frees d.                                            */
    spa_hook_remove(&d->mod_hook);
    teardown(d);
}
static const struct pw_impl_module_events mod_evts={
    PW_VERSION_IMPL_MODULE_EVENTS,.destroy=on_module_destroy
};

/* ═══════════════════════════════════════════════════════════════════
 * Entry point
 * ═══════════════════════════════════════════════════════════════════ */
SPA_EXPORT
int pipewire__module_init(struct pw_impl_module *module, const char *args)
{
    PW_LOG_TOPIC_INIT(mod_topic);
    dbus_threads_init_default();

    struct data *d = calloc(1,sizeof(*d)); if(!d) return -ENOMEM;
    d->module  = module;
    d->context = pw_impl_module_get_context(module);
    d->loop    = pw_context_get_main_loop(d->context);
    atomic_init(&d->running, true);
    atomic_init(&d->connected, false);
    atomic_init(&d->want_connect, false);
    atomic_init(&d->last_audio_ms, 0);
    atomic_init(&d->frames_dropped, 0);

    { int pfd[2]; if(pipe2(pfd,O_CLOEXEC)<0){int res=-errno; free(d); return res;}
      d->wake_r=pfd[0]; d->wake_w=pfd[1]; }

    pthread_mutex_init(&d->state_lock,NULL);
    pthread_mutex_init(&d->name_lock,NULL);
    pthread_mutex_init(&d->srv_lock,NULL);
    pthread_mutex_init(&d->cmd_lock,NULL);
    pthread_cond_init(&d->state_cond,NULL);
    d->cmd_fd = -1;
    ring_init(&d->ring);
    meta_init(&d->meta);
    conn_init(&d->audio_conn);
    conn_init(&d->ctrl_conn);
    conn_init(&d->stats_conn);

    /* Defaults */
    snprintf(d->host,     sizeof(d->host),     "127.0.0.1");
    snprintf(d->sink_name,sizeof(d->sink_name),"AriaCast");
    snprintf(d->sink_desc,sizeof(d->sink_desc),"AriaCast Sink");
    d->port      = AC_STREAM_PORT;
    d->discovery = true;
    d->pause_keepalive_ms = AC_PAUSE_KEEPALIVE_S * 1000;

    struct pw_properties *props = args
        ? pw_properties_new_string(args)
        : pw_properties_new(NULL,NULL);
    if (props) {
        const char *v;
        if((v=pw_properties_get(props,KEY_HOST)))     { snprintf(d->host,sizeof(d->host),"%s",v); d->discovery=false; }
        if((v=pw_properties_get(props,KEY_PORT)) && !parse_port(v,&d->port))
            pw_log_warn("invalid %s \"%s\" – using %u", KEY_PORT, v, d->port);
        if((v=pw_properties_get(props,KEY_NAME)))     { snprintf(d->sink_name,sizeof(d->sink_name),"%s",v); d->name_user_set=true; }
        if((v=pw_properties_get(props,KEY_DESC)))     { snprintf(d->sink_desc,sizeof(d->sink_desc),"%s",v); d->desc_user_set=true; }
        if((v=pw_properties_get(props,KEY_DISCOVERY)))d->discovery=strcmp(v,"false")!=0&&strcmp(v,"0")!=0;
        if((v=pw_properties_get(props,KEY_KEEPALIVE))) {
            char *e; errno=0; long s=strtol(v,&e,10);
            if (errno || e==v || *e || s<0 || s>86400)
                pw_log_warn("invalid %s \"%s\" – using %u", KEY_KEEPALIVE, v, AC_PAUSE_KEEPALIVE_S);
            else
                d->pause_keepalive_ms=(uint32_t)s*1000;
        }
        pw_properties_free(props);
    }

    pw_log_info("server=%s:%u discovery=%s sink=\"%s\" description=\"%s\" pause-keepalive=%us",
                d->host,d->port,d->discovery?"yes":"no",
                d->sink_name,d->sink_desc,d->pause_keepalive_ms/1000);

    snprintf(d->meta.title, sizeof(d->meta.title), "%s", d->sink_name);
    snprintf(d->meta.artist,sizeof(d->meta.artist),"PipeWire");
    d->meta.title_placeholder = true;

    int res;
    d->cmd_fd = eventfd(0, EFD_CLOEXEC|EFD_NONBLOCK);
    if (d->cmd_fd < 0) { res=-errno; pw_log_error("eventfd failed: %s", spa_strerror(res)); goto fail; }

    d->sink_event = pw_loop_add_event(d->loop, on_sink_event, d);
    if (!d->sink_event) { res=-errno; pw_log_error("pw_loop_add_event failed: %s", spa_strerror(res)); goto fail; }

    /* PipeWire core connection */
    d->core = pw_context_connect_self(d->context,NULL,0);
    if (!d->core) { res=-errno; pw_log_error("pw_context_connect_self failed: %s", spa_strerror(res)); goto fail; }
    pw_proxy_add_listener((struct pw_proxy*)d->core, &d->core_proxy_hook,
                          &core_proxy_evts, d);

    /* With discovery, thr_connmgr has the stream created once the
     * server_name is known, so node.name is right from the start.   */
    if (!d->discovery && (res = create_stream(d)) < 0) goto fail;

    /* Start worker threads – last, so a failure above leaves nothing
     * running that could outlive the module.                        */
    void *(*fns[N_THREADS])(void*) = {
        thr_audio_fn, thr_ctrl_fn, thr_stats_fn, thr_meta_fn,
        thr_mpris_fn, thr_watchdog_fn, thr_connmgr_fn,
    };
    for (int i = 0; i < N_THREADS; i++) {
        int e = pthread_create(&d->threads[d->n_threads], NULL, fns[i], d);
        if (e != 0) { res=-e; pw_log_error("pthread_create failed: %s", strerror(e)); goto fail; }
        d->n_threads++;
    }

    pw_impl_module_add_listener(module,&d->mod_hook,&mod_evts,d);
    pw_log_info("module loaded");
    return 0;

fail:
    teardown(d);
    return res;
}
