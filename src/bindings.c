#include <errno.h>
#include <string.h>
#include <janet.h>
#include <zmq.h>

#define FLAG_CLOSED 1

typedef struct {
  void* ctx;
  int flags;
} Ctx;


static void termctx(Ctx *ctx) {
  if(!(ctx->flags & FLAG_CLOSED)) {
    ctx->flags |= FLAG_CLOSED;
    zmq_ctx_shutdown(ctx->ctx);
    zmq_ctx_term(ctx->ctx);
  }
}


static int zmqgcctx(void *p, size_t s) {
  (void) s;
  Ctx *ctx = (Ctx *)p;
  termctx(ctx);
  return 0;
}


static const JanetAbstractType zmq_ctx_type = {
  "zmq.ctx",
  zmqgcctx,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
};


typedef struct {
  void* socket;
} Socket;

static int zmqgcsocket(void *p, size_t s) {
  (void) s;
  (void)zmq_close(p);
  return 0;
}


static const JanetAbstractType zmq_socket_type = {
  "zmq.socket",
  zmqgcsocket,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL
};


static Janet cfun_ctx_new(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 0);
  (void) argv;
  void *ctx_h = zmq_ctx_new();
  Ctx *ctx = (Ctx *) janet_abstract(&zmq_ctx_type, sizeof(Ctx));
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
  int32_t ret = zmq_bind(sock->socket, (char *) endpoint);
  return janet_wrap_integer(ret);
}


static Janet cfun_zmq_send(int32_t argc, Janet *argv) {
  janet_arity(argc, 2, 3);
  int32_t flags = (argc == 3) ? janet_getinteger(argv, 2) : 0;
  Socket *sock = janet_getabstract(argv, 0, &zmq_socket_type);
  int32_t ret = -1;
  switch(janet_type(argv[1])) {
  case JANET_STRING:
  case JANET_BUFFER: {
    JanetByteView bytes = janet_getbytes(argv, 1);
    ret = zmq_send(sock->socket, (void *)bytes.bytes, bytes.len, flags);
  }
    break;
  case JANET_ARRAY:
  case JANET_TUPLE: {
    JanetView view = janet_getindexed(argv, 1);
    int more;
    for(int i = 0; i < view.len; i++) {
      more = (i < (view.len - 1)) ? ZMQ_SNDMORE : 0;
      if(!janet_checktypes(view.items[i], (1 << JANET_STRING) | (1 << JANET_BUFFER))) {
        janet_panic("Msg parts must be String or Buffer types");
      }
      JanetByteView bytes = janet_getbytes(view.items, i);
      ret = zmq_send(sock->socket, (void *) bytes.bytes, bytes.len, flags|more);
    }
  }
    break;
  default:
    janet_panic("argument to zmq/send must be one of string,buffer,array,or tuple");
  }
  return janet_wrap_integer(ret);
}


static JanetString _zmq_recv(void *sock) {
  zmq_msg_t msg;
  int rc = zmq_msg_init(&msg);
  if(rc != 0) {
    janet_panic("Couldn't init msg in _zmq_recv");
  }
  rc = zmq_msg_recv(&msg, sock, 0);
  if(rc == -1) {
    janet_panic("Error in zmq_msg_recv");
  }
  JanetString ret = janet_string((const uint8_t *) zmq_msg_data(&msg), zmq_msg_size(&msg));
  zmq_msg_close(&msg);
  return ret;
}


static Janet cfun_zmq_recv(int32_t argc, Janet *argv) {
  janet_arity(argc, 1, 2);
  int more = 0;
  size_t more_s = sizeof(int);
  int32_t flags = (argc == 2) ? janet_getinteger(argv, 1) : 0;
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  JanetArray *ret = janet_array(16);
  JanetString msg = _zmq_recv(sock->socket);
  janet_array_push(ret, janet_wrap_string(msg));
  int rc = zmq_getsockopt(sock->socket, ZMQ_RCVMORE, &more, &more_s);
  if(rc == -1) {
    janet_panic("Couldn't get RCVMORE socket option in recv");
  }
  while(more) {
    msg = _zmq_recv(sock->socket);
    if(ret->count == ret->capacity) {
      janet_array_ensure(ret, (2 * ret->count), 1);
    }
    janet_array_push(ret, janet_wrap_string(msg));
    int rc = zmq_getsockopt(sock->socket, ZMQ_RCVMORE, &more, &more_s);
    if(rc == -1) {
      janet_panic("Couldn't get RCVMORE socket option in recv");
    }
  }
  return janet_wrap_array(ret);
}

typedef struct {
  Socket *sock;
} WaitState;

static void wait_callback(JanetFiber *fiber, JanetAsyncEvent event) {
  printf("Event: %d\n", event);
  WaitState *state = (WaitState *) fiber->ev_state;
  switch(event) {
  default:
    return;
  case JANET_ASYNC_EVENT_INIT:
  case JANET_ASYNC_EVENT_READ:
  case JANET_ASYNC_EVENT_HUP:
  case JANET_ASYNC_EVENT_ERR:
    {
      int events;
      size_t e_s = sizeof(events);
      int rc = zmq_getsockopt(state->sock->socket, ZMQ_EVENTS, &events, &e_s);
      printf("Events: %d\n", events);
      if(!(events & ZMQ_POLLIN)) {
        return;
      }
    }
    break;
  }
  janet_schedule(fiber, janet_wrap_nil());
  janet_async_end(fiber);
}

static Janet cfun_zmq_wait(int32_t argc, Janet *argv) {
  janet_fixarity(argc, 1);
  WaitState *state = janet_malloc(sizeof(WaitState));
  Socket *sock = (Socket *)janet_getabstract(argv, 0, &zmq_socket_type);
  state->sock = sock;
  int fd;
  size_t fd_s = sizeof(fd);
  int rc = zmq_getsockopt(sock->socket, ZMQ_FD, &fd, &fd_s);
  if(rc == -1) {
    janet_panic("Couldn't get fd from zmq socket");
  }
  printf("fd: %d\n", fd);
  JanetStream *stream = janet_stream((JanetHandle)fd, JANET_STREAM_READABLE, NULL);
  janet_async_start(stream, JANET_ASYNC_LISTEN_READ, wait_callback, state);
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
    {"wait", cfun_zmq_wait, "Wait for socket ready"},
    {NULL, NULL, NULL}
};


JANET_MODULE_ENTRY(JanetTable *env) {
  janet_cfuns(env, "zmq", cfuns);
}
