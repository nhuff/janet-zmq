#include <errno.h>
#include <janet.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <zmq.h>

#define FLAG_CLOSED 1

typedef struct {
  void *ctx;
  int flags;
} Ctx;

static void termctx(Ctx *ctx) {
  if (!(ctx->flags & FLAG_CLOSED)) {
    ctx->flags |= FLAG_CLOSED;
    zmq_ctx_shutdown(ctx->ctx);
    zmq_ctx_term(ctx->ctx);
  }
}

static int zmqgcctx(void *p, size_t s) {
  (void)s;
  Ctx *ctx = (Ctx *)p;
  termctx(ctx);
  return 0;
}

static const JanetAbstractType zmq_ctx_type = {
    "zmq.ctx", zmqgcctx, NULL, NULL, NULL, NULL, NULL,
    NULL,      NULL,     NULL, NULL, NULL, NULL, NULL};

typedef struct {
  void *socket;
  JanetStream *poll_stream;
} Socket;

static int zmqgcsocket(void *p, size_t s) {
  (void)s;
  Socket *sock = (Socket *)p;
  (void)zmq_close(sock->socket);
  return 0;
}

static const JanetAbstractType zmq_socket_type = {
    "zmq.socket", zmqgcsocket, NULL, NULL, NULL, NULL, NULL,
    NULL,         NULL,        NULL, NULL, NULL, NULL, NULL};

static Janet cfun_ctx_new(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 0);
  (void)argv;
  void *ctx_h = zmq_ctx_new();
  Ctx *ctx = (Ctx *)janet_abstract(&zmq_ctx_type, sizeof(Ctx));
  ctx->ctx = ctx_h;
  ctx->flags = 0;
  return janet_wrap_abstract(ctx);
}

static Janet cfun_ctx_term(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Ctx *ctx = janet_getabstract(argv, 0, &zmq_ctx_type);
  termctx(ctx);
  return janet_wrap_nil();
}

static Janet cfun_zmq_socket(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Ctx *ctx = janet_getabstract(argv, 0, &zmq_ctx_type);
  int32_t socket_type = janet_getinteger(argv, 1);
  void *sock = zmq_socket(ctx->ctx, socket_type);
  Socket *ret = (Socket *)janet_abstract(&zmq_socket_type, sizeof(Socket));
  ret->socket = sock;
  int fd;
  size_t fd_size = sizeof(fd);
  int rc = zmq_getsockopt(sock, ZMQ_FD, &fd, &fd_size);
  if (rc == -1) {
    janet_panic("Unable to get poll fd fro socket");
  }
  ret->poll_stream = janet_stream((JanetHandle)dup(fd), JANET_STREAM_READABLE, NULL);
  return janet_wrap_abstract(ret);
}

static Janet cfun_zmq_close(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  Socket *sock = janet_getabstract(argv, 0, &zmq_socket_type);
  int32_t ret = zmq_close(sock->socket);
  return janet_wrap_integer(ret);
}

static Janet cfun_zmq_connect(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  const uint8_t *endpoint = janet_getstring(argv, 1);
  int32_t ret = zmq_connect(sock->socket, (const char *)endpoint);
  return janet_wrap_integer(ret);
}

static Janet cfun_zmq_bind(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  const uint8_t *endpoint = janet_getstring(argv, 1);
  int32_t ret = zmq_bind(sock->socket, (char *)endpoint);
  return janet_wrap_integer(ret);
}

typedef struct {
  Socket *sock;
  JanetArray *msgs;
} SendState;

static void send_callback(JanetFiber *fiber, JanetAsyncEvent event) {
  SendState *state = (SendState *)fiber->ev_state;
  Socket *sock = state->sock;
  int rc;
  switch (event) {
  default:
    return;
  case JANET_ASYNC_EVENT_INIT:
  case JANET_ASYNC_EVENT_READ: {
    int events;
    size_t events_size = sizeof(events);
    rc = zmq_getsockopt(sock->socket, ZMQ_EVENTS, &events, &events_size);
    if (!(events & ZMQ_POLLOUT)) {
      return;
    }
    int more;
    Janet part;
    int count = state->msgs->count;
    for (int i = 0; i < count; i++) {
      more = (i < (count - 1)) ? ZMQ_SNDMORE : 0;
      part = state->msgs->data[i];
      if (!janet_checktypes(part, JANET_TFLAG_BYTES)) {
        janet_panic("Msg parts must be String or Buffer types");
      }
      JanetByteView bytes;
      janet_bytes_view(part, &bytes.bytes, &bytes.len);
      rc = zmq_send(sock->socket, (void *)bytes.bytes, bytes.len, more);
      if (rc == -1) {
        janet_panic("Error in zmq send");
      }
    }
    janet_schedule(fiber, janet_wrap_nil());
    janet_async_end(fiber);
  } break;
  }
}

static Janet cfun_zmq_send(int32_t argc, Janet *argv) {
  janet_arity(argc, 2, 3);
  int32_t flags = (argc == 3) ? janet_getinteger(argv, 2) : 0;
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  int rc;
  SendState *state = janet_malloc(sizeof(SendState));
  state->sock = sock;
  switch (janet_type(argv[1])) {
  case JANET_STRING:
  case JANET_BUFFER: {
    JanetByteView bytes = janet_getbytes(argv, 1);
    state->msgs = janet_array(1);
    janet_array_push(state->msgs,
                     janet_wrap_string(janet_string(bytes.bytes, bytes.len)));
  } break;
  case JANET_ARRAY:
  case JANET_TUPLE: {
    JanetView view = janet_getindexed(argv, 1);
    state->msgs = janet_array(view.len);
    for (int i = 0; i < view.len; i++) {
      if (!janet_checktypes(view.items[i],
                            (1 << JANET_STRING) | (1 << JANET_BUFFER))) {
        janet_panic("Msg parts must be String or Buffer types");
      }
      JanetByteView bytes = janet_getbytes(view.items, i);
      janet_array_push(state->msgs,
                       janet_wrap_string(janet_string(bytes.bytes, bytes.len)));
    }
  } break;
  default:
    janet_panic(
        "argument to zmq/send must be one of string,buffer,array,or tuple");
  }
  janet_async_start(sock->poll_stream, JANET_ASYNC_LISTEN_READ, send_callback,
                    state);
}

static JanetString _zmq_recv(void *sock) {
  zmq_msg_t msg;
  int rc = zmq_msg_init(&msg);
  if (rc != 0) {
    janet_panic("Couldn't init msg in _zmq_recv");
  }
  rc = zmq_msg_recv(&msg, sock, 0);
  if (rc == -1) {
    janet_panic("Error in zmq_msg_recv");
  }
  JanetString ret =
      janet_string((const uint8_t *)zmq_msg_data(&msg), zmq_msg_size(&msg));
  zmq_msg_close(&msg);
  return ret;
}

typedef struct {
  Socket *sock;
} RecvState;

static void recv_callback(JanetFiber *fiber, JanetAsyncEvent event) {
  RecvState *state = fiber->ev_state;
  Socket *sock = state->sock;
  switch (event) {
  default:
    break;
  case JANET_ASYNC_EVENT_INIT:
  case JANET_ASYNC_EVENT_READ: {
    int events;
    size_t events_size = sizeof(events);
    int rc = zmq_getsockopt(sock->socket, ZMQ_EVENTS, &events, &events_size);
    if (!(events & ZMQ_POLLIN)) {
      return;
    }
    int more = 0;
    size_t more_s = sizeof(more);
    JanetArray *ret = janet_array(16);
    JanetString msg = _zmq_recv(sock->socket);
    janet_array_push(ret, janet_wrap_string(msg));
    rc = zmq_getsockopt(sock->socket, ZMQ_RCVMORE, &more, &more_s);
    if (rc == -1) {
      janet_panic("Couldn't get RCVMORE socket option in recv callback");
    }
    while (more) {
      msg = _zmq_recv(sock->socket);
      if (ret->count == ret->capacity) {
        janet_array_ensure(ret, (2 * ret->count), 1);
      }
      janet_array_push(ret, janet_wrap_string(msg));
      int rc = zmq_getsockopt(sock->socket, ZMQ_RCVMORE, &more, &more_s);
      if (rc == -1) {
        janet_panic("Couldn't get RCVMORE socket option in recv callback");
      }
    }
    janet_schedule(fiber, janet_wrap_array(ret));
    janet_async_end(fiber);
  } break;
  }
}

static Janet cfun_zmq_recv(int32_t argc, Janet *argv) {
  janet_arity(argc, 1, 2);
  int32_t flags = (argc == 2) ? janet_getinteger(argv, 1) : 0;
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  RecvState *state = janet_malloc(sizeof(RecvState));
  state->sock = sock;
  janet_async_start(sock->poll_stream, JANET_ASYNC_LISTEN_READ, recv_callback,
                    state);
}

static Janet cfun_zmq_getsockopt(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  int32_t option = janet_getinteger(argv, 1);
  size_t value_len;
  int rc;
  switch (option) {
  default:
    janet_panicf("Unkown getsockopt option %d", option);
    break;
  /* uint64_t */
  case ZMQ_VMCI_BUFFER_MIN_SIZE:
  case ZMQ_VMCI_BUFFER_MAX_SIZE:
  case ZMQ_VMCI_BUFFER_SIZE:
  case ZMQ_AFFINITY: {
    uint64_t val;
    value_len = sizeof(val);
    rc = zmq_getsockopt(sock->socket, option, &val, &value_len);
    if (rc == -1)
      janet_panicf("Couldn't get zmq sock option %d", option);
    return janet_wrap_u64(val);
  }
  /* Binary */
  case ZMQ_ROUTING_ID: {
    uint8_t val[255];
    value_len = sizeof(val);
    rc = zmq_getsockopt(sock->socket, option, &val, &value_len);
    if (rc == -1)
      janet_panicf("Couldn't get zmq sock option %d", option);
    return janet_wrap_string(janet_string(val, value_len));
  }
  /* int */
  case ZMQ_VMCI_CONNECT_TIMEOUT:
  case ZMQ_TYPE:
  case ZMQ_TOS:
  case ZMQ_THREAD_SAFE:
  case ZMQ_TCP_MAXRT:
  case ZMQ_TCP_KEEPALIVE_INTVL:
  case ZMQ_TCP_KEEPALIVE_IDLE:
  case ZMQ_TCP_KEEPALIVE_CNT:
  case ZMQ_TCP_KEEPALIVE:
  case ZMQ_SNDTIMEO:
  case ZMQ_SNDHWM:
  case ZMQ_RCVHWM:
  case ZMQ_RECOVERY_IVL:
  case ZMQ_RECONNECT_IVL_MAX:
  case ZMQ_RECONNECT_IVL:
  case ZMQ_RCVTIMEO:
  case ZMQ_RCVMORE:
  case ZMQ_SNDBUF:
  case ZMQ_RCVBUF:
  case ZMQ_RATE:
  case ZMQ_USE_FD:
  case ZMQ_PLAIN_SERVER:
  case ZMQ_MULTICAST_MAXTPDU:
  case ZMQ_MULTICAST_HOPS:
  case ZMQ_MECHANISM:
  case ZMQ_LINGER:
  case ZMQ_IPV6:
  case ZMQ_INVERT_MATCHING:
  case ZMQ_IMMEDIATE:
  case ZMQ_HANDSHAKE_IVL:
  case ZMQ_GSSAPI_PRINCIPAL_NAMETYPE:
  case ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE:
  case ZMQ_GSSAPI_SERVER:
  case ZMQ_GSSAPI_PLAINTEXT:
  case ZMQ_FD:
  case ZMQ_EVENTS:
  case ZMQ_CONNECT_TIMEOUT:
  case ZMQ_BACKLOG: {
    int val;
    value_len = sizeof(val);
    rc = zmq_getsockopt(sock->socket, option, &val, &value_len);
    if (rc == -1)
      janet_panicf("Couldn't get zmq sock option %d", option);
    return janet_wrap_integer(val);
  }
  /* int64_t */
  case ZMQ_MAXMSGSIZE: {
    int64_t val;
    value_len = sizeof(val);
    rc = zmq_getsockopt(sock->socket, option, &val, &value_len);
    if (rc == -1)
      janet_panicf("Couldn't get zmq sock option %d", option);
    return janet_wrap_s64(val);
  }
  /* Curve Keys */
  case ZMQ_CURVE_SECRETKEY:
  case ZMQ_CURVE_SERVERKEY:
  case ZMQ_CURVE_PUBLICKEY: {
    char val[41];
    value_len = sizeof(val);
    rc = zmq_getsockopt(sock->socket, option, &val, &value_len);
    if (rc == -1)
      janet_panicf("Couldn't get zmq sock option %d", option);
    return janet_wrap_string(val);
  }
  /* string */
  case ZMQ_ZAP_DOMAIN:
  case ZMQ_SOCKS_PROXY:
  case ZMQ_PLAIN_USERNAME:
  case ZMQ_PLAIN_PASSWORD:
  case ZMQ_LAST_ENDPOINT:
  case ZMQ_GSSAPI_PRINCIPAL:
  case ZMQ_GSSAPI_SERVICE_PRINCIPAL:
  case ZMQ_BINDTODEVICE: {
    char val[1024];
    value_len = sizeof(val);
    rc = zmq_getsockopt(sock->socket, option, &val, &value_len);
    if (rc == -1) {
      janet_panicf("Couldn't get zmq sock option %d", option);
    }
    return janet_wrap_string(janet_string((const uint8_t *)val, value_len - 1));
  }
  }
}

static Janet cfun_zmq_setsockopt(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 3);
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  int32_t option = janet_getinteger(argv, 1);
  size_t option_len;
  int rc;
  switch (option) {
  default:
    janet_panicf("Unkown getsockopt option %d", option);
    break;
  /* uint64_t */
  case ZMQ_VMCI_BUFFER_SIZE:
  case ZMQ_VMCI_BUFFER_MIN_SIZE:
  case ZMQ_VMCI_BUFFER_MAX_SIZE:
  case ZMQ_AFFINITY: {
    uint64_t option_val = janet_unwrap_u64(argv[2]);
    option_len = sizeof(option);
    rc = zmq_setsockopt(sock->socket, option, &option_val, option_len);
    if (rc == -1) {
      janet_panicf("Couldn't set zmq socket option %d", option);
    }
  } break;
  /* int */
  case ZMQ_VMCI_CONNECT_TIMEOUT:
  case ZMQ_XPUB_NODROP:
  case ZMQ_XPUB_MANUAL:
  case ZMQ_XPUB_VERBOSER:
  case ZMQ_XPUB_VERBOSE:
  case ZMQ_TOS:
  case ZMQ_TCP_MAXRT:
  case ZMQ_TCP_KEEPALIVE:
  case ZMQ_TCP_KEEPALIVE_CNT:
  case ZMQ_TCP_KEEPALIVE_IDLE:
  case ZMQ_TCP_KEEPALIVE_INTVL:
  case ZMQ_STREAM_NOTIFY:
  case ZMQ_ROUTER_RAW:
  case ZMQ_ROUTER_MANDATORY:
  case ZMQ_ROUTER_HANDOVER:
  case ZMQ_REQ_RELAXED:
  case ZMQ_REQ_CORRELATE:
  case ZMQ_RECONNECT_IVL_MAX:
  case ZMQ_RECONNECT_IVL:
  case ZMQ_RECOVERY_IVL:
  case ZMQ_SNDHWM:
  case ZMQ_RCVHWM:
  case ZMQ_RCVTIMEO:
  case ZMQ_SNDTIMEO:
  case ZMQ_SNDBUF:
  case ZMQ_RCVBUF:
  case ZMQ_RATE:
  case ZMQ_PROBE_ROUTER:
  case ZMQ_USE_FD:
  case ZMQ_PLAIN_SERVER:
  case ZMQ_MULTICAST_MAXTPDU:
  case ZMQ_MULTICAST_HOPS:
  case ZMQ_LINGER:
  case ZMQ_IPV6:
  case ZMQ_INVERT_MATCHING:
  case ZMQ_IMMEDIATE:
  case ZMQ_HEARTBEAT_IVL:
  case ZMQ_HEARTBEAT_TTL:
  case ZMQ_HEARTBEAT_TIMEOUT:
  case ZMQ_HANDSHAKE_IVL:
  case ZMQ_GSSAPI_PRINCIPAL_NAMETYPE:
  case ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE:
  case ZMQ_GSSAPI_SERVER:
  case ZMQ_GSSAPI_PLAINTEXT:
  case ZMQ_CURVE_SERVER:
  case ZMQ_CONNECT_TIMEOUT:
  case ZMQ_CONFLATE:
  case ZMQ_BACKLOG: {
    int32_t option_val = janet_getinteger(argv, 2);
    option_len = sizeof(option_val);
    rc = zmq_setsockopt(sock->socket, option, &option_val, option_len);
    if (rc == -1) {
      janet_panicf("Couldn't set zmq socket option %d", option);
    }
  } break;
    /* int64_t */
  case ZMQ_MAXMSGSIZE: {
    int64_t option_val = janet_unwrap_s64(argv[2]);
    option_len = sizeof(option_val);
    rc = zmq_setsockopt(sock->socket, option, &option_val, option_len);
    if (rc == -1) {
      janet_panicf("Couldn't set zmq socket option %d", option);
    }
  } break;
    /* Curve */
  case ZMQ_CURVE_SERVERKEY:
  case ZMQ_CURVE_SECRETKEY:
  case ZMQ_CURVE_PUBLICKEY: {
    JanetByteView option_val = janet_getbytes(argv, 2);
    option_len = option_val.len;
    if (option_len == 32) {
      /* Binary data */
      rc = zmq_setsockopt(sock->socket, option, &option_val, option_len);
      if (rc == -1) {
        janet_panicf("Couldn't set zmq socket option %d", option);
      }
    } else if (option_len == 40) {
      /* Z85 text encoded data */
      char o_v[41];
      memcpy(o_v, option_val.bytes, 40);
      o_v[40] = '\0';
      rc = zmq_setsockopt(sock->socket, option, o_v, 41);
      if (rc == -1) {
        janet_panicf("Couldn't set zmq socket option %d", option);
      }
    } else {
      janet_panic("Curve key length wrong in setsockopt");
    }
  } break;
  /* Binary */
  case ZMQ_XPUB_WELCOME_MSG:
  case ZMQ_UNSUBSCRIBE:
  case ZMQ_SUBSCRIBE:
  case ZMQ_ROUTING_ID:
  case ZMQ_CONNECT_ROUTING_ID: {
    JanetByteView option_val = janet_getbytes(argv, 2);
    option_len = option_val.len;
    rc = zmq_setsockopt(sock->socket, option, option_val.bytes, option_len);
    if (rc == -1) {
      janet_panicf("Couldn't set zmq socket option %d", option);
    }
  } break;
  /* String */
  case ZMQ_ZAP_DOMAIN:
  case ZMQ_SOCKS_PROXY:
  case ZMQ_PLAIN_USERNAME:
  case ZMQ_PLAIN_PASSWORD:
  case ZMQ_GSSAPI_SERVICE_PRINCIPAL:
  case ZMQ_GSSAPI_PRINCIPAL:
  case ZMQ_BINDTODEVICE: {
    const char *option_val = janet_getcstring(argv, 2);
    option_len = strlen(option_val);
    rc = zmq_setsockopt(sock->socket, option, option_val, option_len);
    if (rc == -1) {
      janet_panicf("Couldn't set zmq socket option %d", option);
    }
  } break;
  }
  return janet_wrap_nil();
}

static Janet cfun_ctx_set(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 3);
  Ctx *ctx = (Ctx *)janet_getabstract(argv, 0, &zmq_ctx_type);
  int32_t option = janet_getinteger(argv, 1);
  int32_t option_value = janet_getinteger(argv, 2);
  int rc = zmq_ctx_set(ctx->ctx, option, option_value);
  if(rc == -1) {
    janet_panicf("Couldn't set option %d on zmq context to %d", option, option_value);
  }
  return janet_wrap_nil();
}

static Janet cfun_ctx_get(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Ctx *ctx = (Ctx *)janet_getabstract(argv, 0, &zmq_ctx_type);
  int32_t option = janet_getinteger(argv, 1);
  int rc = zmq_ctx_get(ctx->ctx, option);
  if(rc == -1) {
    janet_panicf("Couldn't get option %d for zmq context", option);
  }
  return janet_wrap_integer(rc);
}

static Janet cfun_zmq_disconnect(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 2);
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  const char *endpoint = janet_getcstring(argv, 1);
  int rc = zmq_disconnect(sock->socket, endpoint);
  if (rc == -1) {
    janet_panicf("Couldn't disconnect endpoint %s from socket", endpoint);
  }
  return janet_wrap_nil();
}

static const JanetReg cfuns[] = {
    {"ctx_new", cfun_ctx_new, "Create a zmq context"},
    {"ctx_term", cfun_ctx_term, "Terminate a zmq context"},
    {"socket", cfun_zmq_socket, "Create a zmq socket"},
    {"close", cfun_zmq_close, "Close a zmq socket"},
    {"connect", cfun_zmq_connect, "Connect a zmq socket to an endpoint"},
    {"bind", cfun_zmq_bind, "Bind a socket to an enpoint"},
    {"send", cfun_zmq_send, "Queue a message part in a zmq socket"},
    {"recv", cfun_zmq_recv, "Recv a message from a zmq socket"},
    {"getsockopt", cfun_zmq_getsockopt, "Get socket options from a zmq socket"},
    {"setsockopt", cfun_zmq_setsockopt, "Set socket option for a zmq socket"},
    {"ctx_set", cfun_ctx_set, "Set values on zmq context"},
    {"ctx_get", cfun_ctx_get, "Get values from zmq context"},
    {"disconnect", cfun_zmq_disconnect, "Disconnect a socket from an endpoint"},
    {NULL, NULL, NULL}};

JANET_MODULE_ENTRY(JanetTable *env) { janet_cfuns(env, "zmq", cfuns); }
