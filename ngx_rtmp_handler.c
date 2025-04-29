
/*
 * Copyright (C) Roman Arutyunyan
 */


#include <ngx_config.h>
#include <ngx_core.h>
#include "ngx_rtmp.h"
#include "ngx_rtmp_amf.h"
#include <ngx_event.h>

// Defined RTSP session struct
typedef struct {
    int          session_id;      /* a small random or monotonic ID */

    /* (if you ever use UDP fallback, you can keep these) */
    int          client_port_lo;
    int          client_port_hi;
    int          server_port_lo;
    int          server_port_hi;

    uint16_t     rtp_seq;
    uint32_t     rtp_ts;

    /* << NEW fields to support TCP‐interleaved RTP • MUST be here! >> */
    u_char       rtp_channel;     /* “0” from interleaved=0-1 */
    u_char       rtcp_channel;    /* “1” from interleaved=0-1 */
    ngx_event_t  rtp_event;       /* your 40 ms timer event */
} ngx_rtsp_session_t;


static void ngx_rtmp_recv(ngx_event_t *rev);
static void ngx_rtmp_send(ngx_event_t *rev);
static void ngx_rtmp_ping(ngx_event_t *rev);
static ngx_int_t ngx_rtmp_finalize_set_chunk_size(ngx_rtmp_session_t *s);

static void ngx_rtsp_recv(ngx_event_t *rev);        // added function
void ngx_rtsp_init_connection(ngx_connection_t *c); //added function
static void ngx_rtsp_handle_options(ngx_connection_t *c, int cseq);
static void ngx_rtsp_handle_describe(ngx_connection_t *c, int cseq, const char *uri);
static void ngx_rtsp_send_simple_response(ngx_connection_t *c,
    int               cseq,
    int               code,
    const char       *reason,
    const char       *extra_headers);

ngx_uint_t                  ngx_rtmp_naccepted;


ngx_rtmp_bandwidth_t        ngx_rtmp_bw_out;
ngx_rtmp_bandwidth_t        ngx_rtmp_bw_in;


#ifdef NGX_DEBUG
char*
ngx_rtmp_message_type(uint8_t type)
{
    static char*    types[] = {
        "?",
        "chunk_size",
        "abort",
        "ack",
        "user",
        "ack_size",
        "bandwidth",
        "edge",
        "audio",
        "video",
        "?",
        "?",
        "?",
        "?",
        "?",
        "amf3_meta",
        "amf3_shared",
        "amf3_cmd",
        "amf_meta",
        "amf_shared",
        "amf_cmd",
        "?",
        "aggregate"
    };

    return type < sizeof(types) / sizeof(types[0])
        ? types[type]
        : "?";
}


char*
ngx_rtmp_user_message_type(uint16_t evt)
{
    static char*    evts[] = {
        "stream_begin",
        "stream_eof",
        "stream dry",
        "set_buflen",
        "recorded",
        "",
        "ping_request",
        "ping_response",
    };

    return evt < sizeof(evts) / sizeof(evts[0])
        ? evts[evt]
        : "?";
}
#endif


void
ngx_rtmp_cycle(ngx_rtmp_session_t *s)
{
    ngx_connection_t           *c;

    c = s->connection;
    c->read->handler =  ngx_rtmp_recv;
    c->write->handler = ngx_rtmp_send;

    s->ping_evt.data = c;
    s->ping_evt.log = c->log;
    s->ping_evt.handler = ngx_rtmp_ping;
    ngx_rtmp_reset_ping(s);

    ngx_rtmp_recv(c->read);
}


static ngx_chain_t *
ngx_rtmp_alloc_in_buf(ngx_rtmp_session_t *s)
{
    ngx_chain_t        *cl;
    ngx_buf_t          *b;
    size_t              size;

    if ((cl = ngx_alloc_chain_link(s->in_pool)) == NULL
       || (cl->buf = ngx_calloc_buf(s->in_pool)) == NULL)
    {
        return NULL;
    }

    cl->next = NULL;
    b = cl->buf;
    size = s->in_chunk_size + NGX_RTMP_MAX_CHUNK_HEADER;

    b->start = b->last = b->pos = ngx_palloc(s->in_pool, size);
    if (b->start == NULL) {
        return NULL;
    }
    b->end = b->start + size;

    return cl;
}


void
ngx_rtmp_reset_ping(ngx_rtmp_session_t *s)
{
    ngx_rtmp_core_srv_conf_t   *cscf;

    cscf = ngx_rtmp_get_module_srv_conf(s, ngx_rtmp_core_module);
    if (cscf->ping == 0) {
        return;
    }

    s->ping_active = 0;
    s->ping_reset = 0;
    ngx_add_timer(&s->ping_evt, cscf->ping);

    ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
            "ping: wait %Mms", cscf->ping);
}


static void
ngx_rtmp_ping(ngx_event_t *pev)
{
    ngx_connection_t           *c;
    ngx_rtmp_session_t         *s;
    ngx_rtmp_core_srv_conf_t   *cscf;

    c = pev->data;
    s = c->data;

    cscf = ngx_rtmp_get_module_srv_conf(s, ngx_rtmp_core_module);

    /* i/o event has happened; no need to ping */
    if (s->ping_reset) {
        ngx_rtmp_reset_ping(s);
        return;
    }

    if (s->ping_active) {
        ngx_log_error(NGX_LOG_INFO, c->log, 0,
                "ping: unresponded");
        ngx_rtmp_finalize_session(s);
        return;
    }

    if (cscf->busy) {
        ngx_log_error(NGX_LOG_INFO, c->log, 0,
                "ping: not busy between pings");
        ngx_rtmp_finalize_session(s);
        return;
    }

    ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
            "ping: schedule %Mms", cscf->ping_timeout);

    if (ngx_rtmp_send_ping_request(s, (uint32_t)ngx_current_msec) != NGX_OK) {
        ngx_rtmp_finalize_session(s);
        return;
    }

    s->ping_active = 1;
    ngx_add_timer(pev, cscf->ping_timeout);
}


// adding function

/* Called when someone connects on port 554 */
void
ngx_rtsp_init_connection(ngx_connection_t *c)
{
    ngx_log_error(NGX_LOG_WARN, c->log, 0,
                          ">>> RTSP INIT CONNECTION fired, fd=%d  <<<", c->fd);

    ngx_rtsp_session_t *rs;

    ngx_log_error(NGX_LOG_ERR, c->log, 0,
                  ">>> RTSP INIT CONNECTION <<<");

    /* create and zero our per-connection RTSP state */
    rs = ngx_pcalloc(c->pool, sizeof(*rs));
    if (rs == NULL) {
        ngx_close_connection(c);
        ngx_log_error(NGX_LOG_ERR, c->log, 0,
                                  "FAILED to allocate RTSP session state");
        return;
    }

    /* pick a session ID (here just use ngx_time() low bits) */
    rs->session_id = (int)(ngx_time()) & 0xffff;
    rs->rtp_seq = 0;
    rs->rtp_ts = 0;

    ngx_log_error(NGX_LOG_WARN, c->log, 0,
                          "    assigned session_id=%d", rs->session_id);

    /* attach it */
    c->data = rs;

    /* install our read handler and arm the event */
    c->read->handler = ngx_rtsp_recv;
    if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
        ngx_close_connection(c);
    }
}

//added function
static void
ngx_rtsp_send_h264_nal(ngx_connection_t   *c,
                       ngx_rtsp_session_t *rs,
                       const u_char      *nal, 
                       size_t             nal_len,
                       u_char             nal_type,
                       ngx_uint_t         marker)
{
    u_char  buf[1500];
    u_char *p = buf;

    //
    // 1) RTP header (12 bytes)
    //
    *p++ = 0x80;                         // V=2, P=0, X=0, CC=0
    *p++ = (marker ? 0x80 : 0) | 96;     // M=marker, PT=96
    *p++ = (rs->rtp_seq >> 8) & 0xFF;    // sequence #
    *p++ = rs->rtp_seq & 0xFF;
    rs->rtp_seq++;

    // timestamp
    *p++ = (rs->rtp_ts >> 24) & 0xFF;
    *p++ = (rs->rtp_ts >> 16) & 0xFF;
    *p++ = (rs->rtp_ts >>  8) & 0xFF;
    *p++ = rs->rtp_ts & 0xFF;
    rs->rtp_ts += 3600;                  // e.g. 90000/25fps = 3600

    // SSRC (fixed)
    *p++ = 0x12; *p++ = 0x34; *p++ = 0x56; *p++ = 0x78;

    //
    // 2) H.264 NAL header + payload
    //
    //    NAL header: F=0, NRI=0 (or set bits 5–6), type=nal_type
    *p++ = (nal_type & 0x1F);
    ngx_memcpy(p, nal, nal_len);
    p += nal_len;

    //
    // 3) Interleaved TCP header (“$” + channel + 16-bit BE length)
    //
    size_t payload_len = p - buf;
    u_char ihdr[4];
    ihdr[0] = 0x24;                     // ‘$’
    ihdr[1] = (u_char) rs->rtp_channel; // channel (0 for RTP)
    ihdr[2] = (payload_len >> 8) & 0xFF;
    ihdr[3] = payload_len & 0xFF;

    //
    // 4) Send it
    //
    c->send(c, ihdr, 4);
    c->send(c, buf, payload_len);

    ngx_log_error(NGX_LOG_WARN, c->log, 0,
        "RTSP RTP→TCP ch=%d len=%uz seq=%ui ts=%ui",
        rs->rtp_channel, payload_len, rs->rtp_seq, rs->rtp_ts);
}

static void
ngx_rtsp_send_dummy_rtp(ngx_event_t *ev)
{
    ngx_connection_t     *c  = ev->data;
    ngx_rtsp_session_t   *rs = c->data;

    ngx_log_error(NGX_LOG_WARN, c->log, 0,
        "RTSP DUMMY RTP callback: seq=%ui, ts=%ui",
        rs->rtp_seq, rs->rtp_ts);

    /* on first tick, send SPS/PPS */
    static const u_char dummy_sps[] = {
        0x67, 0x42, 0x00, 0x1e, 0xe9, 0x01, 0x40, 0x7b,
        0x20, 0x11, 0x90, 0x00, 0x00, 0x03, 0x00, 0x04
      };
      static const u_char dummy_pps[] = {
        0x68, 0xce, 0x06, 0xe2
      };
      static const u_char dummy_idr[] = {
        0x65, 0x88, 0x84, 0x00, 0x05, 0xff, 0xfb, 0x10
      };

      if (rs->rtp_seq == 0) {
        ngx_rtsp_send_h264_nal(c, rs, dummy_sps, sizeof(dummy_sps), 7, 0);
        ngx_rtsp_send_h264_nal(c, rs, dummy_pps, sizeof(dummy_pps), 8, 1);
    }
    else if (rs->rtp_seq == 1) {
        ngx_rtsp_send_h264_nal(c, rs, dummy_idr, sizeof(dummy_idr), 5, 1);
    }
}

static void
ngx_rtsp_send_simple_response(ngx_connection_t *c,
                              int               cseq,
                              int               code,
                              const char       *status,
                              const char       *extra)
{
    u_char  buf[1024];
    size_t  n;

    /*  
     *  Build the RTSP response:
     *  RTSP/1.0 <code> <status>\r\n
     *  CSeq: <cseq>\r\n
     *  <extra>        (already ends in \r\n…)
     *  \r\n           (blank line to finish headers)
     */
    n = ngx_snprintf(buf, sizeof(buf),
        "RTSP/1.0 %d %s\r\n"
        "CSeq: %d\r\n"
        "%s"
        "\r\n",
        code, status,
        cseq,
        extra ? extra : "")
        - buf;

    /* send it back on the TCP control socket */
    c->send(c, buf, n);
}

// function to handle options

// OPTIONS: advertise which methods we support
static void
ngx_rtsp_handle_options(ngx_connection_t *c, int cseq)
{
    const char *hdrs =
        "Public: OPTIONS, DESCRIBE, SETUP, PLAY, PAUSE, TEARDOWN\r\n";
    ngx_rtsp_send_simple_response(c, cseq, 200, "OK", hdrs);
}

// function to handle describe

// DESCRIBE: return a tiny SDP payload
static void
ngx_rtsp_handle_describe(ngx_connection_t *c, int cseq, const char *uri)
{
    static const char *sdp =
        "v=0\r\n"
        "o=- 0 0 IN IP4 127.0.0.1\r\n"
        "s=nginx-rtsp\r\n"
        "t=0 0\r\n"
        "a=control:*\r\n"
        "m=video 0 RTP/AVP 96\r\n"
        "a=rtpmap:96 H264/90000\r\n"
        "a=control:trackID=0\r\n";

    /* Build headers (no blank line yet) */
    char extra[256];

    ngx_snprintf((u_char*)extra, sizeof(extra),
        "Content-Type: application/sdp\r\n"
        "Content-Length: %uz\r\n",
        ngx_strlen(sdp));

    /* Send the RTSP status + headers + blank line */
    ngx_rtsp_send_simple_response(c, cseq, 200, "OK", extra);

    /* Send the SDP body */
    c->send(c, (u_char*)sdp, ngx_strlen(sdp));
}

static void
ngx_rtsp_handle_setup(ngx_connection_t    *c,
                      int                  cseq,
                      const char         *raw,
                      ngx_rtsp_session_t *rs)
{
    /* lazy‐init session_id if somehow zero */
    if (rs->session_id == 0) {
        rs->session_id = (int)(ngx_time()) & 0xffff;
        ngx_log_error(NGX_LOG_WARN, c->log, 0,
                      "RTSP SETUP: lazily init session_id=%d",
                      rs->session_id);
    }

    /* *** FORCE interleaved channels 0 and 1 *** */
    rs->rtp_channel  = 0;
    rs->rtcp_channel = 1;

    ngx_log_error(NGX_LOG_WARN, c->log, 0,
                  "RTSP SETUP: forcing interleaved=0-1, session=%d",
                  rs->session_id);

    /* now reply with exactly 0–1 */
    u_char extra[128];
    ngx_snprintf(extra, sizeof(extra),
        "Transport: RTP/AVP/TCP;unicast;interleaved=0-1\r\n"
        "Session: %d\r\n",
        rs->session_id);

    ngx_rtsp_send_simple_response(c, cseq, 200, "OK", (char*)extra);
}
static void
ngx_rtsp_handle_play(ngx_connection_t *c,
                     int               cseq,
                     const char       *raw,
                     ngx_rtsp_session_t *rs)
{
    /* send PLAY 200 OK */
    char extra[64];
    ngx_snprintf((u_char*)extra, sizeof(extra),
                 "Session: %d\r\n", rs->session_id);
    ngx_rtsp_send_simple_response(c, cseq, 200, "OK", extra);

    /* prepare rtp_event once */
    rs->rtp_event.handler = ngx_rtsp_send_dummy_rtp;
    rs->rtp_event.data    = c;
    rs->rtp_event.log     = c->log;
    rs->rtp_event.cancelable = 1;

    /* schedule first send in 40ms */
    ngx_add_timer(&rs->rtp_event, 40);
}

// added function

/* Reads raw RTSP request, logs it, and sends back a placeholder */
static void
ngx_rtsp_recv(ngx_event_t *rev)
{
    ngx_connection_t   *c = rev->data;
    ngx_rtsp_session_t *rs = c->data;
    u_char              buf[4096];
    u_char              raw[4096];
    ssize_t             n;
    ssize_t             idx;

    char               *method, *uri, *line;
    int                 cseq = 0;


    if (rev->timedout) {
        ngx_log_error(NGX_LOG_ERR, c->log, 0, "RTSP: recv timeout");
        ngx_close_connection(c);
        return;
    }

    n = c->recv(c, buf, sizeof(buf) - 1);
    if (n <= 0) {
        ngx_close_connection(c);
        return;
    }

    ngx_log_error(NGX_LOG_ERR, c->log, 0,
        "RAW REQUEST DUMP:\n%*s", (int)n, buf);


    /* clamp + NULL */
    {
        ssize_t max = (ssize_t)(sizeof(buf) - 1);
        idx = n < max ? n : max;
        buf[idx] = '\0';
    }

    /* copy exactly the valid portion (including the '\0') */
    ngx_memcpy(raw, buf, idx + 1);

    /* extract CSeq first */
    {
        char *hdr = strstr((char*)buf, "CSeq:");
        if (hdr) {
            char *colon = strchr(hdr, ':');
            if (colon) {
                char *v = colon + 1;
                while (*v==' '||*v=='\t') v++;
                cseq = atoi(v);
            }
        }
    }

    /* parse Request-Line */
    line   = (char*) buf;
    method = strsep(&line, " ");
    uri    = strsep(&line, " ");
    /* drop version */  strsep(&line, "\r\n");

    ngx_log_error(NGX_LOG_ERR, c->log, 0,
                  "RTSP: method='%s' uri='%s' CSeq=%d",
                  method, uri, cseq);

    if (strcmp(method, "OPTIONS") == 0) {
        ngx_rtsp_handle_options(c, cseq);
    }
    else if (strcmp(method, "DESCRIBE") == 0) {
        ngx_rtsp_handle_describe(c, cseq, uri);
    }
    else if (strcmp(method, "SETUP") == 0) {
        ngx_rtsp_handle_setup(c, cseq, (char *) raw, rs);
    }
    else if (strcmp(method, "PLAY") == 0) {
        ngx_rtsp_handle_play(c, cseq, (char *) raw, rs);
    }
    else {
        ngx_rtsp_send_simple_response(c, cseq, 501,
                                      "Not Implemented", NULL);
    }

    /* close only on TEARDOWN */
    if (strcmp(method, "TEARDOWN") == 0) {
        ngx_close_connection(c);
    }
    else {
        /* re-arm the read event for the next request */
        if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
            ngx_close_connection(c);
        }
    }
}

static void
ngx_rtmp_recv(ngx_event_t *rev)
{
    ngx_int_t                   n;
    ngx_connection_t           *c;
    ngx_rtmp_session_t         *s;
    ngx_rtmp_core_srv_conf_t   *cscf;
    ngx_rtmp_header_t          *h;
    ngx_rtmp_stream_t          *st, *st0;
    ngx_chain_t                *in, *head;
    ngx_buf_t                  *b;
    u_char                     *p, *pp, *old_pos;
    size_t                      size, fsize, old_size;
    uint8_t                     fmt, ext;
    uint32_t                    csid, timestamp;

    c = rev->data;
    s = c->data;
    b = NULL;
    old_pos = NULL;
    old_size = 0;
    cscf = ngx_rtmp_get_module_srv_conf(s, ngx_rtmp_core_module);

    if (c->destroyed) {
        return;
    }

    for( ;; ) {

        st = &s->in_streams[s->in_csid];

        /* allocate new buffer */
        if (st->in == NULL) {
            st->in = ngx_rtmp_alloc_in_buf(s);
            if (st->in == NULL) {
                ngx_log_error(NGX_LOG_INFO, c->log, 0,
                        "in buf alloc failed");
                ngx_rtmp_finalize_session(s);
                return;
            }
        }

        h  = &st->hdr;
        in = st->in;
        b  = in->buf;

        if (old_size) {

            ngx_log_debug1(NGX_LOG_DEBUG_RTMP, c->log, 0,
                    "reusing formerly read data: %d", old_size);

            b->pos = b->start;

            size = ngx_min((size_t) (b->end - b->start), old_size);
            b->last = ngx_movemem(b->pos, old_pos, size);

            if (s->in_chunk_size_changing) {
                ngx_rtmp_finalize_set_chunk_size(s);
            }

        } else {

            if (old_pos) {
                b->pos = b->last = b->start;
            }

            n = c->recv(c, b->last, b->end - b->last);

            if (n == NGX_ERROR || n == 0) {
                ngx_rtmp_finalize_session(s);
                return;
            }

            if (n == NGX_AGAIN) {
                if (ngx_handle_read_event(c->read, 0) != NGX_OK) {
                    ngx_rtmp_finalize_session(s);
                }
                return;
            }

            s->ping_reset = 1;
            ngx_rtmp_update_bandwidth(&ngx_rtmp_bw_in, n);
            b->last += n;
            s->in_bytes += n;

            if (s->in_bytes >= 0xf0000000) {
                ngx_log_debug0(NGX_LOG_DEBUG_RTMP, c->log, 0,
                               "resetting byte counter");
                s->in_bytes = 0;
                s->in_last_ack = 0;
            }

            if (s->ack_size && s->in_bytes - s->in_last_ack >= s->ack_size) {

                s->in_last_ack = s->in_bytes;

                ngx_log_debug1(NGX_LOG_DEBUG_RTMP, c->log, 0,
                        "sending RTMP ACK(%uD)", s->in_bytes);

                if (ngx_rtmp_send_ack(s, s->in_bytes)) {
                    ngx_rtmp_finalize_session(s);
                    return;
                }
            }
        }

        old_pos = NULL;
        old_size = 0;

        /* parse headers */
        if (b->pos == b->start) {
            p = b->pos;

            /* chunk basic header */
            fmt  = (*p >> 6) & 0x03;
            csid = *p++ & 0x3f;

            if (csid == 0) {
                if (b->last - p < 1)
                    continue;
                csid = 64;
                csid += *(uint8_t*)p++;

            } else if (csid == 1) {
                if (b->last - p < 2)
                    continue;
                csid = 64;
                csid += *(uint8_t*)p++;
                csid += (uint32_t)256 * (*(uint8_t*)p++);
            }

            ngx_log_debug2(NGX_LOG_DEBUG_RTMP, c->log, 0,
                    "RTMP bheader fmt=%d csid=%D",
                    (int)fmt, csid);

            if (csid >= (uint32_t)cscf->max_streams) {
                ngx_log_error(NGX_LOG_INFO, c->log, 0,
                    "RTMP in chunk stream too big: %D >= %D",
                    csid, cscf->max_streams);
                ngx_rtmp_finalize_session(s);
                return;
            }

            /* link orphan */
            if (s->in_csid == 0) {

                /* unlink from stream #0 */
                st->in = st->in->next;

                /* link to new stream */
                s->in_csid = csid;
                st = &s->in_streams[csid];
                if (st->in == NULL) {
                    in->next = in;
                } else {
                    in->next = st->in->next;
                    st->in->next = in;
                }
                st->in = in;
                h = &st->hdr;
                h->csid = csid;
            }

            ext = st->ext;
            timestamp = st->dtime;
            if (fmt <= 2 ) {
                if (b->last - p < 3)
                    continue;
                /* timestamp:
                 *  big-endian 3b -> little-endian 4b */
                pp = (u_char*)&timestamp;
                pp[2] = *p++;
                pp[1] = *p++;
                pp[0] = *p++;
                pp[3] = 0;

                ext = (timestamp == 0x00ffffff);

                if (fmt <= 1) {
                    if (b->last - p < 4)
                        continue;
                    /* size:
                     *  big-endian 3b -> little-endian 4b
                     * type:
                     *  1b -> 1b*/
                    pp = (u_char*)&h->mlen;
                    pp[2] = *p++;
                    pp[1] = *p++;
                    pp[0] = *p++;
                    pp[3] = 0;
                    h->type = *(uint8_t*)p++;

                    if (fmt == 0) {
                        if (b->last - p < 4)
                            continue;
                        /* stream:
                         *  little-endian 4b -> little-endian 4b */
                        pp = (u_char*)&h->msid;
                        pp[0] = *p++;
                        pp[1] = *p++;
                        pp[2] = *p++;
                        pp[3] = *p++;
                    }
                }
            }

            /* extended header */
            if (ext) {
                if (b->last - p < 4)
                    continue;
                pp = (u_char*)&timestamp;
                pp[3] = *p++;
                pp[2] = *p++;
                pp[1] = *p++;
                pp[0] = *p++;
            }

            if (st->len == 0) {
                /* Messages with type=3 should
                 * never have ext timestamp field
                 * according to standard.
                 * However that's not always the case
                 * in real life */
                st->ext = (ext && cscf->publish_time_fix);
                if (fmt) {
                    st->dtime = timestamp;
                } else {
                    h->timestamp = timestamp;
                    st->dtime = 0;
                }
            }

            ngx_log_debug8(NGX_LOG_DEBUG_RTMP, c->log, 0,
                    "RTMP mheader fmt=%d %s (%d) "
                    "time=%uD+%uD mlen=%D len=%D msid=%D",
                    (int)fmt, ngx_rtmp_message_type(h->type), (int)h->type,
                    h->timestamp, st->dtime, h->mlen, st->len, h->msid);

            /* header done */
            b->pos = p;

            if (h->mlen > cscf->max_message) {
                ngx_log_error(NGX_LOG_INFO, c->log, 0,
                        "too big message: %uz", cscf->max_message);
                ngx_rtmp_finalize_session(s);
                return;
            }
        }

        size = b->last - b->pos;
        fsize = h->mlen - st->len;

        if (size < ngx_min(fsize, s->in_chunk_size))
            continue;

        /* buffer is ready */

        if (fsize > s->in_chunk_size) {
            /* collect fragmented chunks */
            st->len += s->in_chunk_size;
            b->last = b->pos + s->in_chunk_size;
            old_pos = b->last;
            old_size = size - s->in_chunk_size;

        } else {
            /* handle! */
            head = st->in->next;
            st->in->next = NULL;
            b->last = b->pos + fsize;
            old_pos = b->last;
            old_size = size - fsize;
            st->len = 0;
            h->timestamp += st->dtime;

            if (ngx_rtmp_receive_message(s, h, head) != NGX_OK) {
                ngx_rtmp_finalize_session(s);
                return;
            }

            if (s->in_chunk_size_changing) {
                /* copy old data to a new buffer */
                if (!old_size) {
                    ngx_rtmp_finalize_set_chunk_size(s);
                }

            } else {
                /* add used bufs to stream #0 */
                st0 = &s->in_streams[0];
                st->in->next = st0->in;
                st0->in = head;
                st->in = NULL;
            }
        }

        s->in_csid = 0;
    }
}


static void
ngx_rtmp_send(ngx_event_t *wev)
{
    ngx_connection_t           *c;
    ngx_rtmp_session_t         *s;
    ngx_int_t                   n;
    ngx_rtmp_core_srv_conf_t   *cscf;

    c = wev->data;
    s = c->data;

    if (c->destroyed) {
        return;
    }

    if (wev->timedout) {
        ngx_log_error(NGX_LOG_INFO, c->log, NGX_ETIMEDOUT,
                "client timed out");
        c->timedout = 1;
        ngx_rtmp_finalize_session(s);
        return;
    }

    if (wev->timer_set) {
        ngx_del_timer(wev);
    }

    if (s->out_chain == NULL && s->out_pos != s->out_last) {
        s->out_chain = s->out[s->out_pos];
        s->out_bpos = s->out_chain->buf->pos;
    }

    while (s->out_chain) {
        n = c->send(c, s->out_bpos, s->out_chain->buf->last - s->out_bpos);

        if (n == NGX_AGAIN || n == 0) {
            ngx_add_timer(c->write, s->timeout);
            if (ngx_handle_write_event(c->write, 0) != NGX_OK) {
                ngx_rtmp_finalize_session(s);
            }
            return;
        }

        if (n < 0) {
            ngx_rtmp_finalize_session(s);
            return;
        }

        s->out_bytes += n;
        s->ping_reset = 1;
        ngx_rtmp_update_bandwidth(&ngx_rtmp_bw_out, n);
        s->out_bpos += n;
        if (s->out_bpos == s->out_chain->buf->last) {
            s->out_chain = s->out_chain->next;
            if (s->out_chain == NULL) {
                cscf = ngx_rtmp_get_module_srv_conf(s, ngx_rtmp_core_module);
                ngx_rtmp_free_shared_chain(cscf, s->out[s->out_pos]);
                ++s->out_pos;
                s->out_pos %= s->out_queue;
                if (s->out_pos == s->out_last) {
                    break;
                }
                s->out_chain = s->out[s->out_pos];
            }
            s->out_bpos = s->out_chain->buf->pos;
        }
    }

    if (wev->active) {
        ngx_del_event(wev, NGX_WRITE_EVENT, 0);
    }

    ngx_event_process_posted((ngx_cycle_t *) ngx_cycle, &s->posted_dry_events);
}


void
ngx_rtmp_prepare_message(ngx_rtmp_session_t *s, ngx_rtmp_header_t *h,
        ngx_rtmp_header_t *lh, ngx_chain_t *out)
{
    ngx_chain_t                *l;
    u_char                     *p, *pp;
    ngx_int_t                   hsize, thsize;
#if (NGX_DEBUG)
    ngx_int_t                   nbufs;
#endif
    uint32_t                    mlen, timestamp, ext_timestamp;
    static uint8_t              hdrsize[] = { 12, 8, 4, 1 };
    u_char                      th[7];
    ngx_rtmp_core_srv_conf_t   *cscf;
    uint8_t                     fmt;
    ngx_connection_t           *c;

    c = s->connection;
    cscf = ngx_rtmp_get_module_srv_conf(s, ngx_rtmp_core_module);

    if (h->csid >= (uint32_t)cscf->max_streams) {
        ngx_log_error(NGX_LOG_INFO, c->log, 0,
                "RTMP out chunk stream too big: %D >= %D",
                h->csid, cscf->max_streams);
        ngx_rtmp_finalize_session(s);
        return;
    }

    /* detect packet size */
    mlen = 0;
#if (NGX_DEBUG)
    nbufs = 0;
#endif
    for(l = out; l; l = l->next) {
        mlen += (l->buf->last - l->buf->pos);
#if (NGX_DEBUG)
        ++nbufs;
#endif
    }

    fmt = 0;
    if (lh && lh->csid && h->msid == lh->msid) {
        ++fmt;
        if (h->type == lh->type && mlen && mlen == lh->mlen) {
            ++fmt;
            if (h->timestamp == lh->timestamp) {
                ++fmt;
            }
        }
        timestamp = h->timestamp - lh->timestamp;
    } else {
        timestamp = h->timestamp;
    }

    /*if (lh) {
        *lh = *h;
        lh->mlen = mlen;
    }*/

    hsize = hdrsize[fmt];

    ngx_log_debug8(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
            "RTMP prep %s (%d) fmt=%d csid=%uD timestamp=%uD "
            "mlen=%uD msid=%uD nbufs=%d",
            ngx_rtmp_message_type(h->type), (int)h->type, (int)fmt,
            h->csid, timestamp, mlen, h->msid, nbufs);

    ext_timestamp = 0;
    if (timestamp >= 0x00ffffff) {
        ext_timestamp = timestamp;
        timestamp = 0x00ffffff;
        hsize += 4;
    }

    if (h->csid >= 64) {
        ++hsize;
        if (h->csid >= 320) {
            ++hsize;
        }
    }

    /* fill initial header */
    out->buf->pos -= hsize;
    p = out->buf->pos;

    /* basic header */
    *p = (fmt << 6);
    if (h->csid >= 2 && h->csid <= 63) {
        *p++ |= (((uint8_t)h->csid) & 0x3f);
    } else if (h->csid >= 64 && h->csid < 320) {
        ++p;
        *p++ = (uint8_t)(h->csid - 64);
    } else {
        *p++ |= 1;
        *p++ = (uint8_t)(h->csid - 64);
        *p++ = (uint8_t)((h->csid - 64) >> 8);
    }

    /* create fmt3 header for successive fragments */
    thsize = p - out->buf->pos;
    ngx_memcpy(th, out->buf->pos, thsize);
    th[0] |= 0xc0;

    /* message header */
    if (fmt <= 2) {
        pp = (u_char*)&timestamp;
        *p++ = pp[2];
        *p++ = pp[1];
        *p++ = pp[0];
        if (fmt <= 1) {
            pp = (u_char*)&mlen;
            *p++ = pp[2];
            *p++ = pp[1];
            *p++ = pp[0];
            *p++ = h->type;
            if (fmt == 0) {
                pp = (u_char*)&h->msid;
                *p++ = pp[0];
                *p++ = pp[1];
                *p++ = pp[2];
                *p++ = pp[3];
            }
        }
    }

    /* extended header */
    if (ext_timestamp) {
        pp = (u_char*)&ext_timestamp;
        *p++ = pp[3];
        *p++ = pp[2];
        *p++ = pp[1];
        *p++ = pp[0];

        /* This CONTRADICTS the standard
         * but that's the way flash client
         * wants data to be encoded;
         * ffmpeg complains */
        if (cscf->play_time_fix) {
            ngx_memcpy(&th[thsize], p - 4, 4);
            thsize += 4;
        }
    }

    /* append headers to successive fragments */
    for(out = out->next; out; out = out->next) {
        out->buf->pos -= thsize;
        ngx_memcpy(out->buf->pos, th, thsize);
    }
}


ngx_int_t
ngx_rtmp_send_message(ngx_rtmp_session_t *s, ngx_chain_t *out,
        ngx_uint_t priority)
{
    ngx_uint_t                      nmsg;

    nmsg = (s->out_last - s->out_pos) % s->out_queue + 1;

    if (priority > 3) {
        priority = 3;
    }

    /* drop packet?
     * Note we always leave 1 slot free */
    if (nmsg + priority * s->out_queue / 4 >= s->out_queue) {
        ngx_log_debug2(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                "RTMP drop message bufs=%ui, priority=%ui",
                nmsg, priority);
        return NGX_AGAIN;
    }

    s->out[s->out_last++] = out;
    s->out_last %= s->out_queue;

    ngx_rtmp_acquire_shared_chain(out);

    ngx_log_debug3(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
            "RTMP send nmsg=%ui, priority=%ui #%ui",
            nmsg, priority, s->out_last);

    if (priority && s->out_buffer && nmsg < s->out_cork) {
        return NGX_OK;
    }

    if (!s->connection->write->active) {
        ngx_rtmp_send(s->connection->write);
        /*return ngx_add_event(s->connection->write, NGX_WRITE_EVENT, NGX_CLEAR_EVENT);*/
    }

    return NGX_OK;
}


ngx_int_t
ngx_rtmp_receive_message(ngx_rtmp_session_t *s,
        ngx_rtmp_header_t *h, ngx_chain_t *in)
{
    ngx_rtmp_core_main_conf_t  *cmcf;
    ngx_array_t                *evhs;
    size_t                      n;
    ngx_rtmp_handler_pt        *evh;

    cmcf = ngx_rtmp_get_module_main_conf(s, ngx_rtmp_core_module);

#ifdef NGX_DEBUG
    {
        int             nbufs;
        ngx_chain_t    *ch;

        for(nbufs = 1, ch = in;
                ch->next;
                ch = ch->next, ++nbufs);

        ngx_log_debug7(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                "RTMP recv %s (%d) csid=%D timestamp=%D "
                "mlen=%D msid=%D nbufs=%d",
                ngx_rtmp_message_type(h->type), (int)h->type,
                h->csid, h->timestamp, h->mlen, h->msid, nbufs);
    }
#endif

    if (h->type > NGX_RTMP_MSG_MAX) {
        ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                "unexpected RTMP message type: %d", (int)h->type);
        return NGX_OK;
    }

    evhs = &cmcf->events[h->type];
    evh = evhs->elts;

    ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
            "nhandlers: %d", evhs->nelts);

    for(n = 0; n < evhs->nelts; ++n, ++evh) {
        if (!evh) {
            continue;
        }
        ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                "calling handler %d", n);

        switch ((*evh)(s, h, in)) {
            case NGX_ERROR:
                ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
                        "handler %d failed", n);
                return NGX_ERROR;
            case NGX_DONE:
                return NGX_OK;
        }
    }

    return NGX_OK;
}


ngx_int_t
ngx_rtmp_set_chunk_size(ngx_rtmp_session_t *s, ngx_uint_t size)
{
    ngx_rtmp_core_srv_conf_t           *cscf;
    ngx_chain_t                        *li, *fli, *lo, *flo;
    ngx_buf_t                          *bi, *bo;
    ngx_int_t                           n;

    ngx_log_debug1(NGX_LOG_DEBUG_RTMP, s->connection->log, 0,
        "setting chunk_size=%ui", size);

    if (size > NGX_RTMP_MAX_CHUNK_SIZE) {
        ngx_log_error(NGX_LOG_ALERT, s->connection->log, 0,
                      "too big RTMP chunk size:%ui", size);
        return NGX_ERROR;
    }

    cscf = ngx_rtmp_get_module_srv_conf(s, ngx_rtmp_core_module);

    s->in_old_pool = s->in_pool;
    s->in_chunk_size = size;
    s->in_pool = ngx_create_pool(4096, s->connection->log);

    /* copy existing chunk data */
    if (s->in_old_pool) {
        s->in_chunk_size_changing = 1;
        s->in_streams[0].in = NULL;

        for(n = 1; n < cscf->max_streams; ++n) {
            /* stream buffer is circular
             * for all streams except for the current one
             * (which caused this chunk size change);
             * we can simply ignore it */
            li = s->in_streams[n].in;
            if (li == NULL || li->next == NULL) {
                s->in_streams[n].in = NULL;
                continue;
            }
            /* move from last to the first */
            li = li->next;
            fli = li;
            lo = ngx_rtmp_alloc_in_buf(s);
            if (lo == NULL) {
                return NGX_ERROR;
            }
            flo = lo;
            for ( ;; ) {
                bi = li->buf;
                bo = lo->buf;

                if (bo->end - bo->last >= bi->last - bi->pos) {
                    bo->last = ngx_cpymem(bo->last, bi->pos,
                            bi->last - bi->pos);
                    li = li->next;
                    if (li == fli)  {
                        lo->next = flo;
                        s->in_streams[n].in = lo;
                        break;
                    }
                    continue;
                }

                bi->pos += (ngx_cpymem(bo->last, bi->pos,
                            bo->end - bo->last) - bo->last);
                lo->next = ngx_rtmp_alloc_in_buf(s);
                lo = lo->next;
                if (lo == NULL) {
                    return NGX_ERROR;
                }
            }
        }
    }

    return NGX_OK;
}


static ngx_int_t
ngx_rtmp_finalize_set_chunk_size(ngx_rtmp_session_t *s)
{
    if (s->in_chunk_size_changing && s->in_old_pool) {
        ngx_destroy_pool(s->in_old_pool);
        s->in_old_pool = NULL;
        s->in_chunk_size_changing = 0;
    }
    return NGX_OK;
}


