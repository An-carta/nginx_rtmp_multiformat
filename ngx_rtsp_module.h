// ngx_rtsp_module.h
#ifndef NGX_RTSP_MODULE_H
#define NGX_RTSP_MODULE_H

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_event.h>         // For ngx_event_t
#include <ngx_connection.h>    // For ngx_connection_t
#include <ngx_http.h>          // If using HTTP features
#include <ngx_rtmp.h>          // If integrating with RTMP
#include <ngx_event_connect.h>  // For ngx_handle_read_event()

/* ─── Module Structure Declarations ───────────────────────────────────────── */
typedef struct ngx_rtsp_session_s ngx_rtsp_session_t;
typedef struct ngx_rtsp_stream_s ngx_rtsp_stream_t;

/* shared across all sessions */
extern ngx_queue_t  streams_head;

/* master‐process init hook */
ngx_int_t ngx_rtsp_streams_init_module(ngx_cycle_t *cycle);

/* called when someone connects on port 554 */
void ngx_rtsp_init_connection(ngx_connection_t *c);



#endif  // NGX_RTSP_MODULE_H