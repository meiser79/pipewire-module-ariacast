/* module-ariacast.c  –  PipeWire sink module for AriaCast (spec v1.0)
 *
 * Design:
 *  - Registers as Audio/Sink so it appears in output device selectors
 *  - Connects to the AriaCast server only when streaming starts
 *    (user routes audio here), disconnects when stopped/idle
 *  - MPRIS2 DBus listener for metadata: xesam:title, xesam:artist,
 *    xesam:album, mpris:artUrl. mpris:trackid is used to detect track
 *    changes and clear stale album/artwork.
 *
 * SPDX-License-Identifier: MIT
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dbus/dbus.h>
#include <errno.h>
#include <inttypes.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/uio.h>
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
#define META_JSON_MAX   3584

/* ═══════════════════════════════════════════════════════════════════
 * Base64
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

/* ═══════════════════════════════════════════════════════════════════
 * WebSocket helpers
 * ═══════════════════════════════════════════════════════════════════ */
#define WS_FIN   0x80
#define WS_TEXT  0x01
#define WS_BIN   0x02
#define WS_CLOSE 0x08
#define WS_PING  0x09
#define WS_PONG  0x0A

static void ws_rand_key(char k[25]) {
    uint8_t r[16]; struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    srand((unsigned)(ts.tv_nsec ^ ts.tv_sec ^ (uintptr_t)k));
    for (int i = 0; i < 16; i++) r[i] = rand() & 0xff;
    b64_encode(r, 16, k); k[24] = 0;
}
static int io_read(int fd, void *b, size_t n) {
    size_t d = 0; uint8_t *p = b;
    while (d < n) { ssize_t r = read(fd, p+d, n-d); if (r <= 0) return -1; d += r; }
    return 0;
}
static int io_write(int fd, const void *b, size_t n) {
    size_t d = 0; const uint8_t *p = b;
    while (d < n) { ssize_t r = write(fd, p+d, n-d); if (r <= 0) return -1; d += r; }
    return 0;
}
static int ws_send_frame(int fd, uint8_t op, const void *pay, size_t len) {
    uint8_t h[14]; int hl = 0;
    h[hl++] = WS_FIN | op;
    if      (len <= 125)   h[hl++] = 0x80|(uint8_t)len;
    else if (len <= 65535) { h[hl++]=0x80|126; h[hl++]=(len>>8)&0xff; h[hl++]=len&0xff; }
    else                   { h[hl++]=0x80|127; for(int i=7;i>=0;i--) h[hl++]=(len>>(i*8))&0xff; }
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    uint32_t m = (uint32_t)(ts.tv_nsec ^ (uintptr_t)pay ^ len);
    uint8_t mk[4] = { m&0xff, (m>>8)&0xff, (m>>16)&0xff, (m>>24)&0xff };
    h[hl++]=mk[0]; h[hl++]=mk[1]; h[hl++]=mk[2]; h[hl++]=mk[3];
    uint8_t *mx = malloc(len); if (!mx) return -1;
    for (size_t i = 0; i < len; i++) mx[i] = ((const uint8_t*)pay)[i] ^ mk[i&3];
    struct iovec iv[2] = {{h,(size_t)hl},{mx,len}};
    ssize_t r = writev(fd, iv, 2); free(mx); return r < 0 ? -1 : 0;
}
typedef struct { uint8_t op; uint8_t *data; size_t len; } ws_frame_t;
static int ws_recv_frame(int fd, ws_frame_t *f) {
    uint8_t h[2]; if (io_read(fd,h,2)<0) return -1;
    f->op = h[0]&0x0f; bool mk = (h[1]&0x80)!=0;
    uint64_t len = h[1]&0x7f;
    if (len==126){ uint8_t e[2]; if(io_read(fd,e,2)<0)return -1; len=((uint64_t)e[0]<<8)|e[1]; }
    else if (len==127){ uint8_t e[8]; if(io_read(fd,e,8)<0)return -1; len=0; for(int i=0;i<8;i++) len=(len<<8)|e[i]; }
    uint8_t mask[4]={0}; if (mk && io_read(fd,mask,4)<0) return -1;
    f->data = malloc(len+1); if (!f->data) return -1;
    if (len>0 && io_read(fd,f->data,(size_t)len)<0) { free(f->data); return -1; }
    if (mk) for (uint64_t i=0;i<len;i++) f->data[i]^=mask[i&3];
    f->data[len]=0; f->len=(size_t)len; return 0;
}

/* ── TCP + WebSocket upgrade ──────────────────────────────────────── */
static int ws_connect(const char *host, uint16_t port, const char *path, int *out) {
    char ps[8]; snprintf(ps,sizeof(ps),"%u",port);
    struct addrinfo hi={.ai_family=AF_UNSPEC,.ai_socktype=SOCK_STREAM}, *res=NULL;
    if (getaddrinfo(host,ps,&hi,&res)!=0) return -1;
    int fd=-1;
    for (struct addrinfo *ai=res; ai; ai=ai->ai_next) {
        fd=socket(ai->ai_family,ai->ai_socktype,ai->ai_protocol); if(fd<0) continue;
        int one=1; setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
        struct timeval tv={.tv_sec=AC_HS_TIMEOUT};
        setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
        setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
        if (connect(fd,ai->ai_addr,ai->ai_addrlen)==0) break;
        close(fd); fd=-1;
    }
    freeaddrinfo(res); if (fd<0) return -1;
    char key[25]; ws_rand_key(key);
    char req[512];
    int rl = snprintf(req,sizeof(req),
        "GET %s HTTP/1.1\r\nHost: %s:%u\r\nUpgrade: websocket\r\n"
        "Connection: Upgrade\r\nSec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n", path,host,port,key);
    if (io_write(fd,req,(size_t)rl)<0) { close(fd); return -1; }
    char resp[2048]; int n=0;
    while (n<(int)sizeof(resp)-1) {
        if (read(fd,resp+n,1)<=0) { close(fd); return -1; }
        n++; if (n>=4 && memcmp(resp+n-4,"\r\n\r\n",4)==0) break;
    }
    resp[n]=0; if (!strstr(resp,"101")) { close(fd); return -1; }
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
    pthread_cond_t  not_empty, not_full;
    bool closing;
} ring_t;

static void ring_init(ring_t *r) {
    memset(r,0,sizeof(*r));
    pthread_mutex_init(&r->lock,NULL);
    pthread_cond_init(&r->not_empty,NULL);
    pthread_cond_init(&r->not_full,NULL);
}
static void ring_close(ring_t *r) {
    pthread_mutex_lock(&r->lock); r->closing=true;
    pthread_cond_broadcast(&r->not_empty);
    pthread_cond_broadcast(&r->not_full);
    pthread_mutex_unlock(&r->lock);
}
static void ring_destroy(ring_t *r) {
    pthread_mutex_destroy(&r->lock);
    pthread_cond_destroy(&r->not_empty);
    pthread_cond_destroy(&r->not_full);
}
static bool ring_push(ring_t *r, const uint8_t *f) {
    pthread_mutex_lock(&r->lock);
    while (r->count==RING_CAP && !r->closing) pthread_cond_wait(&r->not_full,&r->lock);
    if (r->closing) { pthread_mutex_unlock(&r->lock); return false; }
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
    pthread_cond_signal(&r->not_full); pthread_mutex_unlock(&r->lock); return true;
}

/* ═══════════════════════════════════════════════════════════════════
 * Wakeup pipe + interruptible sleep
 * ═══════════════════════════════════════════════════════════════════ */
static uint64_t now_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts);
    return (uint64_t)ts.tv_sec*1000ULL + (uint64_t)(ts.tv_nsec/1000000ULL);
}
static void wake(int wfd) {
    char c=1; ssize_t r=write(wfd,&c,1); (void)r;
}
static void isleep_ms(int rfd, long ms) {
    fd_set fds; FD_ZERO(&fds); FD_SET(rfd,&fds);
    struct timeval tv={ms/1000,(ms%1000)*1000};
    int r = select(rfd+1,&fds,NULL,NULL,&tv);
    if (r > 0 && FD_ISSET(rfd,&fds)) {
        /* Woken by wake() – drain the byte(s). Non-blocking is not
         * needed here since FD_ISSET guarantees data is available. */
        char buf[64]; ssize_t n=read(rfd,buf,sizeof(buf)); (void)n;
    }
    /* r == 0: timeout elapsed, nothing to drain – return normally.
     * r  < 0: interrupted/error – also return normally.            */
}

/* ═══════════════════════════════════════════════════════════════════
 * Per-connection state
 * ═══════════════════════════════════════════════════════════════════ */
typedef struct { int fd; pthread_mutex_t lock; } conn_t;
static void conn_init(conn_t *c)     { c->fd=-1; pthread_mutex_init(&c->lock,NULL); }
static void conn_destroy(conn_t *c)  { pthread_mutex_destroy(&c->lock); }
static int  conn_get(conn_t *c)      { pthread_mutex_lock(&c->lock); int f=c->fd; pthread_mutex_unlock(&c->lock); return f; }
static void conn_set(conn_t *c,int f){ pthread_mutex_lock(&c->lock); if(c->fd>=0) close(c->fd); c->fd=f; pthread_mutex_unlock(&c->lock); }
static void conn_close(conn_t *c)    {
    pthread_mutex_lock(&c->lock);
    if (c->fd>=0) { ws_send_frame(c->fd,WS_CLOSE,NULL,0); close(c->fd); c->fd=-1; }
    pthread_mutex_unlock(&c->lock);
}

/* ═══════════════════════════════════════════════════════════════════
 * Metadata state
 * ═══════════════════════════════════════════════════════════════════ */
typedef struct {
    char    title[256], artist[256], album[256], artwork[512];
    char    last_trackid[256]; /* mpris:trackid of the last processed update */
    int64_t dur_ms, pos_ms;
    bool    playing;
    pthread_mutex_t lock;
} meta_t;

static void meta_init(meta_t *m)    { memset(m,0,sizeof(*m)); pthread_mutex_init(&m->lock,NULL); }
static void meta_destroy(meta_t *m) { pthread_mutex_destroy(&m->lock); }

static int meta_to_json(meta_t *m, char *buf, size_t sz) {
    pthread_mutex_lock(&m->lock);
    const char *p = m->playing ? "true" : "false";
    int n = snprintf(buf,(int)sz,
        "{\"title\":\"%s\",\"artist\":\"%s\",\"album\":\"%s\","
        "\"artworkUrl\":\"%s\",\"artwork_url\":\"%s\","
        "\"durationMs\":%" PRId64 ",\"duration_ms\":%" PRId64 ","
        "\"positionMs\":%" PRId64 ",\"position_ms\":%" PRId64 ","
        "\"isPlaying\":%s,\"is_playing\":%s}",
        m->title, m->artist, m->album, m->artwork, m->artwork,
        m->dur_ms, m->dur_ms, m->pos_ms, m->pos_ms, p, p);
    pthread_mutex_unlock(&m->lock);
    return n;
}

/* ═══════════════════════════════════════════════════════════════════
 * HTTP POST /metadata  (metadata.md)
 * ═══════════════════════════════════════════════════════════════════ */
static void http_post_meta(const char *host, uint16_t port, const char *mj) {
    char body[META_JSON_MAX+16];
    int  bl = snprintf(body,sizeof(body),"{\"data\":%s}",mj);
    char req[META_JSON_MAX+256];
    int  rl = snprintf(req,sizeof(req),
        "POST /metadata HTTP/1.0\r\nHost: %s:%u\r\n"
        "Content-Type: application/json\r\nContent-Length: %d\r\n"
        "Connection: close\r\n\r\n%s", host,port,bl,body);
    char ps[8]; snprintf(ps,sizeof(ps),"%u",port);
    struct addrinfo hi={.ai_family=AF_UNSPEC,.ai_socktype=SOCK_STREAM}, *res=NULL;
    int gai = getaddrinfo(host,ps,&hi,&res);
    if (gai!=0) {
        pw_log_warn("/metadata: getaddrinfo(%s:%u) failed: %s", host, port, gai_strerror(gai));
        return;
    }
    int fd=-1;
    for (struct addrinfo *ai=res; ai; ai=ai->ai_next) {
        fd=socket(ai->ai_family,ai->ai_socktype,ai->ai_protocol); if(fd<0) continue;
        struct timeval tv={.tv_sec=2};
        setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&tv,sizeof(tv));
        setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
        if (connect(fd,ai->ai_addr,ai->ai_addrlen)==0) break;
        close(fd); fd=-1;
    }
    freeaddrinfo(res);
    if (fd<0) {
        pw_log_warn("/metadata: connect to %s:%u failed: %s", host, port, strerror(errno));
        return;
    }
    if (io_write(fd,req,(size_t)rl)<0) {
        pw_log_warn("/metadata: write to %s:%u failed: %s", host, port, strerror(errno));
        close(fd);
        return;
    }
    char rb[128]={0}; ssize_t u=read(fd,rb,sizeof(rb)-1);
    if (u>0) {
        rb[u]=0;
        char *eol=strchr(rb,'\r'); if(eol)*eol=0;
        pw_log_debug("/metadata POST %s:%u -> %s", host, port, rb);
    } else {
        pw_log_debug("/metadata POST %s:%u -> (no response, %s)", host, port, strerror(errno));
    }
    close(fd);
}

/* ═══════════════════════════════════════════════════════════════════
 * Discovery  (discovery.md)
 * ═══════════════════════════════════════════════════════════════════ */
typedef struct { char host[64]; uint16_t port; char name[128]; } ac_srv_t;

static int discover(ac_srv_t *srv) {
    for (int i=1; i<=AC_DISC_TRIES; i++) {
        pw_log_info("discovery attempt %d/%d", i, AC_DISC_TRIES);
        int s=socket(AF_INET,SOCK_DGRAM,IPPROTO_UDP); if(s<0) goto next;
        int one=1;
        setsockopt(s,SOL_SOCKET,SO_BROADCAST,&one,sizeof(one));
        setsockopt(s,SOL_SOCKET,SO_REUSEADDR,&one,sizeof(one));
        struct sockaddr_in lo={.sin_family=AF_INET,.sin_port=0,.sin_addr.s_addr=INADDR_ANY};
        bind(s,(struct sockaddr*)&lo,sizeof(lo));
        struct sockaddr_in dst={.sin_family=AF_INET,
                                .sin_port=htons(AC_DISC_PORT),
                                .sin_addr.s_addr=INADDR_BROADCAST};
        const char *msg="DISCOVER_AUDIOCAST";
        if (sendto(s,msg,strlen(msg),0,(struct sockaddr*)&dst,sizeof(dst))<0)
            { pw_log_warn("discovery sendto: %s",strerror(errno)); close(s); goto next; }
        struct timeval tv={.tv_sec=2};
        setsockopt(s,SOL_SOCKET,SO_RCVTIMEO,&tv,sizeof(tv));
        char buf[512]; struct sockaddr_in from; socklen_t fl=sizeof(from);
        ssize_t n=recvfrom(s,buf,sizeof(buf)-1,0,(struct sockaddr*)&from,&fl);
        close(s);
        if (n<=0) { pw_log_debug("no response"); goto next; }
        buf[n]=0; pw_log_info("discovery: %s",buf);
        srv->port=AC_STREAM_PORT; srv->host[0]=srv->name[0]=0;
        char *p;
        if ((p=strstr(buf,"\"ip\"")) && (p=strchr(p,':')) && (p=strchr(p,'"'))) {
            p++; char *e=strchr(p,'"');
            if (e && (size_t)(e-p)<sizeof(srv->host))
                { memcpy(srv->host,p,(size_t)(e-p)); srv->host[e-p]=0; }
        }
        if (!srv->host[0]) inet_ntop(AF_INET,&from.sin_addr,srv->host,sizeof(srv->host));
        if ((p=strstr(buf,"\"port\"")) && (p=strchr(p,':'))) srv->port=(uint16_t)atoi(p+1);
        if ((p=strstr(buf,"\"server_name\"")) && (p=strchr(p,':')) && (p=strchr(p,'"'))) {
            p++; char *e=strchr(p,'"');
            if (e && (size_t)(e-p)<sizeof(srv->name))
                { memcpy(srv->name,p,(size_t)(e-p)); srv->name[e-p]=0; }
        }
        pw_log_info("found \"%s\" at %s:%u",srv->name,srv->host,srv->port);
        return 0;
next:
        if (i<AC_DISC_TRIES) sleep(AC_DISC_DELAY);
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
struct data {
    struct pw_impl_module *module;
    struct pw_context     *context;
    struct spa_hook        mod_hook;

    char     host[256];
    uint16_t port;
    char     sink_name[128];
    char     sink_desc[256];
    bool     discovery;
    bool     name_user_set;    /* ariacast.sink.name was configured     */
    bool     desc_user_set;    /* ariacast.sink.description configured  */
    pthread_mutex_t name_lock; /* guards sink_name/sink_desc            */

    /* PipeWire */
    struct pw_stream   *stream;
    struct spa_hook     stream_hook;
    struct pw_core     *core;
    struct spa_hook     core_proxy_hook;
    bool                core_gone; /* set by core_proxy_destroy() before
                                     * pw_context_destroy() frees the
                                     * underlying core/stream objects   */

    /* Worker threads */
    pthread_t  thr_audio, thr_ctrl, thr_stats, thr_meta, thr_mpris, thr_connmgr, thr_watchdog;
    bool       running;
    bool       destroying;  /* guards against re-entrant .destroy calls */

    /* Wakeup pipe – write 1 byte to unblock any isleep_ms() */
    int  wake_r, wake_w;

    /* Connection state.
     * want_connect is driven by ACTUAL AUDIO ACTIVITY (on_process),
     * not merely by the stream being linked – PipeWire sets
     * STREAMING as soon as the sink is linked, even while silent. */
    bool       connected;       /* true after successful connect  */
    bool       want_connect;    /* true while non-silent audio is flowing */
    bool       discover_joined; /* true once discovery has run (or was skipped) */
    pthread_mutex_t state_lock;
    pthread_cond_t  state_cond;

    /* Timestamp (CLOCK_MONOTONIC, ms) of the last non-silent audio frame.
     * Written by the RT process callback, read by thr_watchdog.       */
    uint64_t   last_audio_ms;

    ring_t  ring;
    meta_t  meta;
    conn_t  audio_conn, ctrl_conn, stats_conn;

    /* Frame accumulation buffer for the AriaCast 20ms/3840B protocol
     * frame size. PipeWire's audioconvert negotiates the stream to
     * AC_RATE/AC_CHANNELS/S16LE (see EnumFormat in pipewire__module_init),
     * so on_process() always receives audio already at 48kHz - no
     * resampling is done here.                                        */
    uint8_t  accum[AC_FRAME_BYTES];
    size_t   accum_fill;
};

/* ═══════════════════════════════════════════════════════════════════
 * MPRIS2 metadata helpers
 *
 * Only standard MPRIS2 keys are used:
 *   xesam:title    -> title
 *   xesam:artist   -> artist (first element of the array)
 *   xesam:album    -> album
 *   mpris:artUrl   -> artworkUrl
 *   mpris:trackid  -> used only to detect track changes, so album/
 *                     artwork can be cleared when a new track has
 *                     none of its own (no stale cover/album shown).
 *
 * Empty-string values are treated as "field not present", since some
 * players send empty strings for fields they have cleared.
 * ═══════════════════════════════════════════════════════════════════ */

static bool dbus_iter_get_string(DBusMessageIter *it, char *out, size_t sz) {
    int t = dbus_message_iter_get_arg_type(it);
    if (t == DBUS_TYPE_STRING || t == DBUS_TYPE_OBJECT_PATH) {
        const char *s = NULL; dbus_message_iter_get_basic(it, &s);
        if (s) { snprintf(out, sz, "%s", s); return true; }
    } else if (t == DBUS_TYPE_ARRAY) {
        DBusMessageIter sub; dbus_message_iter_recurse(it, &sub);
        if (dbus_message_iter_get_arg_type(&sub) == DBUS_TYPE_STRING) {
            const char *s = NULL; dbus_message_iter_get_basic(&sub, &s);
            if (s) { snprintf(out, sz, "%s", s); return true; }
        }
    }
    return false;
}

static void mpris_process_metadata(struct data *d, DBusMessageIter *dict) {
    char title[256]   = "";
    char artist[256]  = "";
    char album[256]   = "";
    char artwork[512] = "";
    char trackid[256] = "";
    bool has_title = false, has_artist = false, has_album = false,
         has_artwork = false, has_trackid = false;

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
next_key:
        dbus_message_iter_next(dict);
    }

    /* Treat empty strings as "not present" */
    if (has_title   && title[0]   == '\0') has_title   = false;
    if (has_artist  && artist[0]  == '\0') has_artist  = false;
    if (has_album   && album[0]   == '\0') has_album   = false;
    if (has_artwork && artwork[0] == '\0') has_artwork = false;

    bool changed = false;
    pthread_mutex_lock(&d->meta.lock);

    /* mpris:trackid changes whenever the player switches to a different
     * track/stream. Used to clear album/artwork that don't apply to the
     * new track when the new update doesn't provide its own.           */
    bool track_changed = has_trackid &&
        strcmp(d->meta.last_trackid, trackid) != 0;
    if (has_trackid)
        snprintf(d->meta.last_trackid, sizeof(d->meta.last_trackid), "%s", trackid);

    if (has_title  && strcmp(d->meta.title,  title)  != 0)
        { snprintf(d->meta.title,  sizeof(d->meta.title),  "%s", title);  changed=true; }
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

    pthread_mutex_unlock(&d->meta.lock);

    if (changed) {
        pw_log_info("MPRIS meta: title='%s' artist='%s' -> %s:%u",
                    title, artist, d->host, d->port);
        char mj[META_JSON_MAX]; meta_to_json(&d->meta, mj, sizeof(mj));
        http_post_meta(d->host, d->port, mj);
    }
}
static void mpris_handle_props_changed(struct data *d, DBusMessage *msg) {
    DBusMessageIter iter;
    if (!dbus_message_iter_init(msg, &iter))
        return; /* PropertiesChanged with empty body */

    /* arg 0: interface name string – skip */
    if (dbus_message_iter_get_arg_type(&iter)==DBUS_TYPE_STRING)
        dbus_message_iter_next(&iter);

    /* arg 1: a{sv} changed properties */
    if (dbus_message_iter_get_arg_type(&iter) != DBUS_TYPE_ARRAY)
        return;
    DBusMessageIter outer_dict; dbus_message_iter_recurse(&iter, &outer_dict);

    while (dbus_message_iter_get_arg_type(&outer_dict) == DBUS_TYPE_DICT_ENTRY) {
        DBusMessageIter entry, var;
        dbus_message_iter_recurse(&outer_dict, &entry);
        const char *key=NULL; dbus_message_iter_get_basic(&entry,&key);
        dbus_message_iter_next(&entry);
        if (!key || strcmp(key,"Metadata")!=0) { dbus_message_iter_next(&outer_dict); continue; }
        if (dbus_message_iter_get_arg_type(&entry)!=DBUS_TYPE_VARIANT) break;
        dbus_message_iter_recurse(&entry,&var);
        if (dbus_message_iter_get_arg_type(&var)==DBUS_TYPE_ARRAY) {
            DBusMessageIter meta_dict; dbus_message_iter_recurse(&var,&meta_dict);
            mpris_process_metadata(d, &meta_dict);
        }
        break;
    }
}

/* ── MPRIS thread ─────────────────────────────────────────────────── */
static void *thr_mpris_fn(void *arg) {
    struct data *d = arg;
    DBusError err; dbus_error_init(&err);

    DBusConnection *conn = dbus_bus_get(DBUS_BUS_SESSION, &err);
    if (!conn || dbus_error_is_set(&err)) {
        pw_log_warn("MPRIS: DBus connect failed: %s",
                    dbus_error_is_set(&err)?err.message:"unknown");
        dbus_error_free(&err);
        return NULL;
    }

    dbus_bus_add_match(conn,
        "type='signal',"
        "interface='org.freedesktop.DBus.Properties',"
        "member='PropertiesChanged'", &err);
    dbus_connection_flush(conn);
    if (dbus_error_is_set(&err)) {
        pw_log_warn("MPRIS: add_match failed: %s", err.message);
        dbus_error_free(&err);
        dbus_connection_unref(conn);
        return NULL;
    }
    pw_log_info("MPRIS2 listener started");

    while (d->running) {
        if (!dbus_connection_get_is_connected(conn)) {
            pw_log_warn("MPRIS: DBus connection lost – reconnecting");
            dbus_connection_unref(conn);
            isleep_ms(d->wake_r, 1000);
            if (!d->running) break;

            dbus_error_init(&err);
            conn = dbus_bus_get(DBUS_BUS_SESSION, &err);
            if (!conn || dbus_error_is_set(&err)) {
                pw_log_warn("MPRIS: reconnect failed: %s",
                            dbus_error_is_set(&err)?err.message:"unknown");
                dbus_error_free(&err);
                isleep_ms(d->wake_r, 2000);
                continue;
            }
            dbus_bus_add_match(conn,
                "type='signal',"
                "interface='org.freedesktop.DBus.Properties',"
                "member='PropertiesChanged'", &err);
            dbus_connection_flush(conn);
            if (dbus_error_is_set(&err)) {
                pw_log_warn("MPRIS: add_match after reconnect failed: %s", err.message);
                dbus_error_free(&err);
                dbus_connection_unref(conn);
                isleep_ms(d->wake_r, 2000);
                continue;
            }
            pw_log_info("MPRIS2 listener reconnected");
            continue;
        }

        dbus_connection_read_write_dispatch(conn, 300);
        DBusMessage *msg;
        while ((msg=dbus_connection_pop_message(conn))!=NULL) {
            if (dbus_message_is_signal(msg,"org.freedesktop.DBus.Properties","PropertiesChanged"))
                mpris_handle_props_changed(d, msg);
            dbus_message_unref(msg);
        }
    }

    if (dbus_connection_get_is_connected(conn))
        dbus_connection_unref(conn);
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
    pw_log_info("connecting to AriaCast %s:%u ...", d->host, d->port);

    int fd = audio_connect(d->host, d->port);
    if (fd < 0) { pw_log_warn("/audio: connect failed"); return false; }
    conn_set(&d->audio_conn, fd);

    int cfd = ctrl_connect(d->host, d->port);
    if (cfd >= 0) conn_set(&d->ctrl_conn, cfd);

    int sfd = stats_connect(d->host, d->port);
    if (sfd >= 0) conn_set(&d->stats_conn, sfd);

    char mj[META_JSON_MAX]; meta_to_json(&d->meta, mj, sizeof(mj));
    http_post_meta(d->host, d->port, mj);

    d->connected = true;
    pthread_mutex_lock(&d->meta.lock); d->meta.playing = true; pthread_mutex_unlock(&d->meta.lock);
    pw_log_info("connected to AriaCast");
    return true;
}

static void do_disconnect(struct data *d) {
    conn_close(&d->audio_conn);
    conn_close(&d->ctrl_conn);
    conn_close(&d->stats_conn);
    /* Drop any buffered frames so the next session starts clean */
    pthread_mutex_lock(&d->ring.lock);
    d->ring.head = d->ring.tail = d->ring.count = 0;
    pthread_mutex_unlock(&d->ring.lock);
    d->connected = false;
    pthread_mutex_lock(&d->meta.lock); d->meta.playing = false; pthread_mutex_unlock(&d->meta.lock);
    pw_log_info("disconnected from AriaCast");
}

/* ── Sink naming from the discovered server_name ──────────────────
 * "Wohnzimmer Lautsprecher"  ->  node name  "ariacast-wohnzimmer-lautsprecher"
 *                                description "Wohnzimmer Lautsprecher"
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

/* Runs on the PipeWire main loop – safe place to touch the stream. */
static int do_update_sink_props(struct spa_loop *loop, bool async, uint32_t seq,
                                const void *data, size_t size, void *user_data) {
    struct data *d = user_data;
    (void)loop; (void)async; (void)seq; (void)data; (void)size;
    if (!d->stream || d->core_gone) return 0;
    char name[128], desc[256];
    pthread_mutex_lock(&d->name_lock);
    snprintf(name, sizeof(name), "%s", d->sink_name);
    snprintf(desc, sizeof(desc), "%s", d->sink_desc);
    pthread_mutex_unlock(&d->name_lock);
    char node[160]; slugify(name, node, sizeof(node));
    if (!node[0]) snprintf(node, sizeof(node), "ariacast-sink");
    struct spa_dict_item items[] = {
        SPA_DICT_ITEM_INIT(PW_KEY_NODE_NAME,        node),
        SPA_DICT_ITEM_INIT(PW_KEY_NODE_NICK,        name),
        SPA_DICT_ITEM_INIT(PW_KEY_NODE_DESCRIPTION, desc),
        SPA_DICT_ITEM_INIT(PW_KEY_MEDIA_NAME,       desc),
    };
    struct spa_dict dict = SPA_DICT_INIT(items, SPA_N_ELEMENTS(items));
    pw_stream_update_properties(d->stream, &dict);
    pw_log_info("sink renamed: node=\"%s\" description=\"%s\"", node, desc);
    return 0;
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
        snprintf(d->meta.title, sizeof(d->meta.title), "%s", d->sink_desc);
        pthread_mutex_unlock(&d->meta.lock);
    }
    pthread_mutex_unlock(&d->name_lock);

    if (!changed) return;
    struct pw_loop *loop = pw_context_get_main_loop(d->context);
    if (loop)
        pw_loop_invoke(loop, do_update_sink_props, 0, NULL, 0, false, d);
}

/* Re-run UDP discovery and update d->host/d->port if a server responds.
 * Only called when ariacast.server.host was NOT explicitly configured. */
static void redo_discovery(struct data *d) {
    if (!d->discovery) return;
    ac_srv_t srv = {0};
    if (discover(&srv) == 0) {
        if (strcmp(d->host,srv.host)!=0 || d->port!=srv.port)
            pw_log_info("rediscovered server: %s:%u (was %s:%u)",
                        srv.host, srv.port, d->host, d->port);
        snprintf(d->host, sizeof(d->host), "%s", srv.host);
        d->port = srv.port;
        if (srv.name[0])
            apply_server_name(d, srv.name);
    }
}

/* ── Audio sender thread ─────────────────────────────────────────── */
static void *thr_audio_fn(void *arg) {
    struct data *d = arg;
    uint8_t frame[AC_FRAME_BYTES];
    pw_log_info("audio sender thread started");
    while (d->running) {
        if (!ring_pop(&d->ring, frame)) {
            if (!d->running) break;
            isleep_ms(d->wake_r, 50);
            continue;
        }
        int fd = conn_get(&d->audio_conn);
        if (fd < 0) continue; /* not connected yet – discard frame */
        if (ws_send_frame(fd, WS_BIN, frame, AC_FRAME_BYTES) < 0) {
            pw_log_warn("/audio: send failed");
            conn_close(&d->audio_conn); /* thr_connmgr will reconnect */
        }
    }
    pw_log_info("audio sender thread stopped"); return NULL;
}

/* ── Idle watchdog: clears want_connect after AC_IDLE_MS of silence ── */
static void *thr_watchdog_fn(void *arg) {
    struct data *d = arg;
    while (d->running) {
        isleep_ms(d->wake_r, 250);
        if (!d->running) break;
        pthread_mutex_lock(&d->state_lock);
        if (d->want_connect && d->last_audio_ms != 0 &&
            now_ms() - d->last_audio_ms > AC_IDLE_MS) {
            d->want_connect = false;
            pthread_cond_broadcast(&d->state_cond);
            pw_log_info("idle for %d ms – disconnecting", AC_IDLE_MS);
        }
        pthread_mutex_unlock(&d->state_lock);
    }
    return NULL;
}

static void handle_ctrl_action(struct data *d, const char *j) {
    pthread_mutex_lock(&d->meta.lock);
    if      (strstr(j,"\"play\""))   d->meta.playing = true;
    else if (strstr(j,"\"pause\""))  d->meta.playing = false;
    else if (strstr(j,"\"toggle\"")) d->meta.playing = !d->meta.playing;
    else if (strstr(j,"\"stop\""))   d->meta.playing = false;
    pthread_mutex_unlock(&d->meta.lock);
}

static void *thr_ctrl_fn(void *arg) {
    struct data *d = arg;
    while (d->running) {
        int fd = conn_get(&d->ctrl_conn);
        if (fd < 0) { isleep_ms(d->wake_r, 500); continue; }
        ws_frame_t f={0};
        if (ws_recv_frame(fd,&f)<0) { conn_close(&d->ctrl_conn); continue; }
        if (f.op==WS_TEXT  && f.data) handle_ctrl_action(d,(char*)f.data);
        if (f.op==WS_PING  && f.data) ws_send_frame(fd,WS_PONG,f.data,f.len);
        if (f.op==WS_CLOSE) { free(f.data); conn_close(&d->ctrl_conn); continue; }
        free(f.data);
    }
    return NULL;
}

static void *thr_stats_fn(void *arg) {
    struct data *d = arg;
    /* /stats is optional – only the Python server implements it.
     * The Go server returns 404; we simply stop trying after the
     * first failure rather than looping forever.                  */
    while (d->running) {
        int fd = conn_get(&d->stats_conn);
        if (fd < 0) {
            /* Not connected or not supported – sleep and check again
             * only if we are actively streaming (connected).        */
            isleep_ms(d->wake_r, 5000);
            continue;
        }
        ws_frame_t f={0};
        if (ws_recv_frame(fd,&f)<0) {
            /* Connection dropped – close and don't retry; /stats is
             * best-effort only.                                      */
            conn_close(&d->stats_conn);
            pw_log_debug("/stats disconnected (optional endpoint)");
            /* Sleep until next connect cycle */
            isleep_ms(d->wake_r, 30000);
            continue;
        }
        if (f.op==WS_TEXT && f.data) pw_log_debug("stats: %s",(char*)f.data);
        if (f.op==WS_PING && f.data) ws_send_frame(fd,WS_PONG,f.data,f.len);
        free(f.data);
    }
    return NULL;
}

static void *thr_meta_fn(void *arg) {
    struct data *d = arg;
    while (d->running) {
        if (d->connected) {
            char mj[META_JSON_MAX]; meta_to_json(&d->meta, mj, sizeof(mj));
            http_post_meta(d->host, d->port, mj);
        }
        for (int i=0; i<10 && d->running; i++) isleep_ms(d->wake_r, 1000);
    }
    return NULL;
}

/* ── Connection manager thread ────────────────────────────────────── */
static void *thr_connmgr_fn(void *arg) {
    struct data *d = arg;
    pw_log_info("connection manager started");

    /* Run discovery once up-front so the sink can be named after the
     * server's "server_name" even before any audio is routed to it. */
    if (d->discovery && !d->discover_joined) {
        redo_discovery(d);
        d->discover_joined = true;
    }

    while (d->running) {
        /* ── Wait until audio activity requests a connection ─────── */
        pthread_mutex_lock(&d->state_lock);
        while (d->running && !d->want_connect)
            pthread_cond_wait(&d->state_cond, &d->state_lock);
        bool wc = d->want_connect;
        pthread_mutex_unlock(&d->state_lock);
        if (!d->running) break;
        if (!wc) continue;

        /* Run discovery once, lazily, on the very first connection
         * attempt (only if no host was explicitly configured).      */
        if (!d->discover_joined) {
            if (d->discovery) redo_discovery(d);
            d->discover_joined = true;
        }

        /* ── Connect with exponential backoff ─────────────────────
         * Re-check want_connect between attempts so a brief audio
         * blip doesn't cause us to keep retrying after the user
         * has already moved on.                                     */
        int attempt = 0;
        bool connected = false;
        while (d->running) {
            pthread_mutex_lock(&d->state_lock);
            wc = d->want_connect;
            pthread_mutex_unlock(&d->state_lock);
            if (!wc) break;

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
        if (!d->running) { if (connected) do_disconnect(d); break; }
        if (!connected) continue; /* want_connect went false before connecting */

        /* ── Connected: monitor health + want_connect ─────────────── */
        while (d->running) {
            pthread_mutex_lock(&d->state_lock);
            struct timespec ts; clock_gettime(CLOCK_REALTIME,&ts);
            ts.tv_nsec += 500L*1000000L;
            if (ts.tv_nsec >= 1000000000L) { ts.tv_nsec-=1000000000L; ts.tv_sec+=1; }
            pthread_cond_timedwait(&d->state_cond,&d->state_lock,&ts);
            wc = d->want_connect;
            pthread_mutex_unlock(&d->state_lock);

            if (!d->running || !wc) break;
            if (conn_get(&d->audio_conn) < 0) {
                pw_log_warn("/audio connection lost while active – reconnecting");
                break; /* fall through to reconnect below */
            }
        }

        if (!d->running) { if (d->connected) do_disconnect(d); break; }

        pthread_mutex_lock(&d->state_lock);
        wc = d->want_connect;
        pthread_mutex_unlock(&d->state_lock);

        if (!wc) {
            /* User stopped / went idle */
            if (d->connected) do_disconnect(d);
        }
        /* else: /audio dropped but want_connect still true – loop back
         * to the top, which re-enters the backoff-connect block since
         * want_connect is still true (do_connect will be retried).   */
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
 * on struct data).                                                     */
static void push_audio(struct data *d, const uint8_t *pcm, uint32_t nb){
    size_t off = 0;
    while (off < nb) {
        size_t need = AC_FRAME_BYTES - d->accum_fill;
        size_t take = (nb - off) < need ? (nb - off) : need;
        memcpy(d->accum + d->accum_fill, pcm + off, take);
        d->accum_fill += take;
        off += take;
        if (d->accum_fill == AC_FRAME_BYTES) {
            ring_push(&d->ring, d->accum);
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

static void on_process(void *arg){
    struct data *d=arg;
    struct pw_buffer *b=pw_stream_dequeue_buffer(d->stream); if(!b) return;
    struct spa_buffer *buf=b->buffer;
    uint32_t size=buf->datas[0].chunk->size;

    if (size>0) {
        if (buf_is_audible((uint8_t*)buf->datas[0].data, size)) {
            pthread_mutex_lock(&d->state_lock);
            d->last_audio_ms = now_ms();
            if (!d->want_connect) {
                d->want_connect = true;
                pthread_cond_broadcast(&d->state_cond);
            }
            pthread_mutex_unlock(&d->state_lock);
        }
        if (d->connected)
            push_audio(d,(uint8_t*)buf->datas[0].data,size);
    }
    pw_stream_queue_buffer(d->stream,b);
}

static void on_state_changed(void *arg,
    enum pw_stream_state old, enum pw_stream_state state, const char *err)
{
    struct data *d=arg;
    pw_log_info("stream: %s → %s%s%s",
        pw_stream_state_as_string(old),pw_stream_state_as_string(state),
        err?"(":"",err?err:"");

    /* When the stream is fully unlinked from the graph, disconnect
     * immediately regardless of the idle timer.  STREAMING/PAUSED
     * transitions while linked are NOT used to gate the connection –
     * that is driven purely by audio activity in on_process().      */
    if (state==PW_STREAM_STATE_UNCONNECTED ||
        state==PW_STREAM_STATE_ERROR) {
        pthread_mutex_lock(&d->state_lock);
        d->want_connect = false;
        d->last_audio_ms = 0;
        pthread_cond_broadcast(&d->state_cond);
        pthread_mutex_unlock(&d->state_lock);
    }
    (void)old;
}

static const struct pw_stream_events stream_evts={
    PW_VERSION_STREAM_EVENTS,
    .param_changed=on_param_changed,
    .process=on_process,
    .state_changed=on_state_changed,
};

/* ═══════════════════════════════════════════════════════════════════
 * Core proxy destroy listener
 *
 * pw_context_connect_self() returns a pw_core* which is also a
 * pw_proxy*. During full daemon shutdown, pw_context_destroy() tears
 * down internal client connections (and the proxies/streams that
 * belong to them) BEFORE invoking each module's .destroy hook. If we
 * then call pw_stream_destroy()/pw_core_disconnect() on these already
 *-freed objects, pw_stream_destroy() crashes inside the library.
 *
 * By listening for this proxy's `destroy` event, we learn that
 * PipeWire has already (or is about to) reclaim these objects, and
 * skip our own teardown calls in that case - while still performing
 * them normally on a regular module unload, avoiding any leak there.
 * ═══════════════════════════════════════════════════════════════════ */
static void core_proxy_destroy(void *arg) {
    struct data *d = arg;
    d->core_gone = true;
    pw_log_debug("core proxy destroyed by daemon – skipping manual stream/core teardown");
}
static const struct pw_proxy_events core_proxy_evts = {
    PW_VERSION_PROXY_EVENTS,
    .destroy = core_proxy_destroy,
};

/* ═══════════════════════════════════════════════════════════════════
 * Module destroy
 * ═══════════════════════════════════════════════════════════════════ */
static void on_module_destroy(void *arg){
    struct data *d=arg;

    /* PipeWire can invoke a module's .destroy hook more than once during
     * shutdown (observed: once for the normal module unload, and again
     * from pw_context_destroy()'s final cleanup with a stale/corrupt
     * argument once *d has already been freed). Guard against running
     * the teardown twice and against operating on a freed d.          */
    if (d->destroying) {
        pw_log_debug("destroy callback re-entered – ignoring");
        return;
    }
    d->destroying = true;

    pw_log_info("destroying");

    d->running=false;

    /* Wake all sleeping threads */
    wake(d->wake_w);

    /* Signal connection manager to stop waiting */
    pthread_mutex_lock(&d->state_lock);
    d->want_connect=false;
    pthread_cond_broadcast(&d->state_cond);
    pthread_mutex_unlock(&d->state_lock);

    /* Close sockets so ws_recv_frame returns */
    conn_close(&d->audio_conn);
    conn_close(&d->ctrl_conn);
    conn_close(&d->stats_conn);

    /* Wake ring_pop */
    ring_close(&d->ring);

    /* Tear down our PipeWire objects, unless the core_proxy_destroy()
     * listener already told us that pw_context_destroy() has reclaimed
     * them as part of a full daemon shutdown (see core_proxy_destroy()
     * and core_proxy_evts above for why this check is necessary - calling
     * pw_stream_destroy() on an object PipeWire is concurrently freeing
     * crashes inside the library, even on the very first call).         */
    spa_hook_remove(&d->stream_hook);
    if (!d->core_gone) {
        if (d->stream) pw_stream_destroy(d->stream);
        if (d->core)   pw_core_disconnect(d->core);
    } else {
        pw_log_debug("skipping stream/core teardown – already reclaimed by daemon shutdown");
    }
    spa_hook_remove(&d->core_proxy_hook);
    d->stream = NULL;
    d->core   = NULL;

    pthread_join(d->thr_connmgr, NULL);
    pthread_join(d->thr_audio,   NULL);
    pthread_join(d->thr_ctrl,    NULL);
    pthread_join(d->thr_stats,   NULL);
    pthread_join(d->thr_meta,    NULL);
    pthread_join(d->thr_mpris,   NULL);
    pthread_join(d->thr_watchdog,NULL);

    close(d->wake_r); close(d->wake_w);

    /* d->mod_hook is embedded in *d. It MUST be removed from
     * pw_impl_module's internal hook list before free(d) below –
     * otherwise that list keeps a dangling pointer into freed memory,
     * which pw_context_destroy() dereferences during the daemon's own
     * shutdown, causing a use-after-free crash in pw_stream_destroy()
     * with garbage arguments (the original bug this comment replaces).
     * spa_hook_remove() only unlinks from the list (prev/next) and is
     * safe to call from within the hook's own callback.               */
    spa_hook_remove(&d->mod_hook);
    ring_destroy(&d->ring);
    conn_destroy(&d->audio_conn);
    conn_destroy(&d->ctrl_conn);
    conn_destroy(&d->stats_conn);
    pthread_mutex_destroy(&d->state_lock);
    pthread_mutex_destroy(&d->name_lock);
    pthread_cond_destroy(&d->state_cond);
    meta_destroy(&d->meta);

    /* Intentionally not calling free(d) - see comment on `destroying`
     * field and at the top of this function. */
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

    struct data *d = calloc(1,sizeof(*d)); if(!d) return -ENOMEM;
    d->module  = module;
    d->context = pw_impl_module_get_context(module);
    d->running = true;

    { int pfd[2]; if(pipe(pfd)<0){free(d);return -errno;}
      d->wake_r=pfd[0]; d->wake_w=pfd[1]; }

    pthread_mutex_init(&d->state_lock,NULL);
    pthread_mutex_init(&d->name_lock,NULL);
    pthread_cond_init(&d->state_cond,NULL);

    /* Defaults */
    strncpy(d->host,     "127.0.0.1",    sizeof(d->host)-1);
    strncpy(d->sink_name,"AriaCast",     sizeof(d->sink_name)-1);
    strncpy(d->sink_desc,"AriaCast Sink",sizeof(d->sink_desc)-1);
    d->port      = AC_STREAM_PORT;
    d->discovery = true;

    struct pw_properties *props = args
        ? pw_properties_new_string(args)
        : pw_properties_new(NULL,NULL);
    if (props) {
        const char *v;
        if((v=pw_properties_get(props,KEY_HOST)))     { snprintf(d->host,sizeof(d->host),"%s",v); d->discovery=false; }
        if((v=pw_properties_get(props,KEY_PORT)))     d->port=(uint16_t)atoi(v);
        if((v=pw_properties_get(props,KEY_NAME)))     { snprintf(d->sink_name,sizeof(d->sink_name),"%s",v); d->name_user_set=true; }
        if((v=pw_properties_get(props,KEY_DESC)))     { snprintf(d->sink_desc,sizeof(d->sink_desc),"%s",v); d->desc_user_set=true; }
        if((v=pw_properties_get(props,KEY_DISCOVERY)))d->discovery=strcmp(v,"false")!=0&&strcmp(v,"0")!=0;
        pw_properties_free(props);
    }

    pw_log_info("server=%s:%u discovery=%s sink=\"%s\" description=\"%s\"",
                d->host,d->port,d->discovery?"yes":"no",
                d->sink_name,d->sink_desc);

    ring_init(&d->ring);
    meta_init(&d->meta);
    conn_init(&d->audio_conn);
    conn_init(&d->ctrl_conn);
    conn_init(&d->stats_conn);
    strncpy(d->meta.title, d->sink_name,255);
    strncpy(d->meta.artist,"PipeWire",  255);

    /* Start worker threads */
    pthread_create(&d->thr_audio, NULL, thr_audio_fn,   d);
    pthread_create(&d->thr_ctrl,  NULL, thr_ctrl_fn,    d);
    pthread_create(&d->thr_stats, NULL, thr_stats_fn,   d);
    pthread_create(&d->thr_meta,  NULL, thr_meta_fn,    d);
    pthread_create(&d->thr_mpris, NULL, thr_mpris_fn,   d);
    pthread_create(&d->thr_watchdog, NULL, thr_watchdog_fn, d);

    pthread_create(&d->thr_connmgr, NULL, thr_connmgr_fn, d);

    /* PipeWire stream */
    d->core = pw_context_connect_self(d->context,NULL,0);
    if (!d->core) {
        pw_log_error("pw_context_connect_self failed: %m");
        d->running=false; wake(d->wake_w);
        ring_close(&d->ring);
        pthread_cond_broadcast(&d->state_cond);
        return -EIO;
    }
    pw_proxy_add_listener((struct pw_proxy*)d->core, &d->core_proxy_hook,
                          &core_proxy_evts, d);

    char node_name[160];
    pthread_mutex_lock(&d->name_lock);
    slugify(d->sink_name, node_name, sizeof(node_name));
    if (!node_name[0]) snprintf(node_name, sizeof(node_name), "ariacast-sink");

    struct pw_properties *sp = pw_properties_new(
        PW_KEY_DEVICE_ICON_NAME, "audio-speakers",
        PW_KEY_MEDIA_TYPE,       "Audio",
        PW_KEY_MEDIA_CATEGORY,   "Capture",
        PW_KEY_MEDIA_CLASS,      "Audio/Sink",
        PW_KEY_MEDIA_ROLE,       "Music",
        PW_KEY_NODE_NAME,        node_name,
        PW_KEY_NODE_NICK,        d->sink_name,
        PW_KEY_NODE_DESCRIPTION, d->sink_desc,
        PW_KEY_NODE_VIRTUAL,     "true",
        NULL);

    d->stream=pw_stream_new(d->core,d->sink_name,sp);
    pthread_mutex_unlock(&d->name_lock);
    if(!d->stream){ pw_log_error("pw_stream_new failed"); return -ENOMEM; }
    pw_stream_add_listener(d->stream,&d->stream_hook,&stream_evts,d);

    uint8_t buf[1024]; struct spa_pod_builder b=SPA_POD_BUILDER_INIT(buf,sizeof(buf));
    const struct spa_pod *params[1];
    params[0]=spa_format_audio_raw_build(&b,SPA_PARAM_EnumFormat,
        &SPA_AUDIO_INFO_RAW_INIT(.format=SPA_AUDIO_FORMAT_S16_LE,
                                  .channels=AC_CHANNELS,.rate=AC_RATE));
    pw_stream_connect(d->stream,PW_DIRECTION_INPUT,PW_ID_ANY,
        PW_STREAM_FLAG_MAP_BUFFERS|PW_STREAM_FLAG_RT_PROCESS,
        params,1);

    pw_impl_module_add_listener(module,&d->mod_hook,&mod_evts,d);
    pw_log_info("module loaded, sink \"%s\" registered (idle until selected)",d->sink_name);
    return 0;
}
