(import zmq-native)

# Socket types
(def ZMQ_PAIR 0)
(def ZMQ_PUB 1)
(def ZMQ_REQ 3)
(def ZMQ_SUB 2)
(def ZMQ_REP 4)
(def ZMQ_DEALER 5)
(def ZMQ_ROUTER 6)
(def ZMQ_PULL 7)
(def ZMQ_PUSH 8)
(def ZMQ_XPUB 9)
(def ZMQ_XSUB 10)
(def ZMQ_STREAM 11)

# Socket options
(def ZMQ_AFFINITY 4)
(def ZMQ_ROUTING_ID 5)
(def ZMQ_SUBSCRIBE 6)
(def ZMQ_UNSUBSCRIBE 7)
(def ZMQ_RATE 8)
(def ZMQ_RECOVERY_IVL 9)
(def ZMQ_SNDBUF 11)
(def ZMQ_RCVBUF 12)
(def ZMQ_RCVMORE 13)
(def ZMQ_FD 14)
(def ZMQ_EVENTS 15)
(def ZMQ_TYPE 16)
(def ZMQ_LINGER 17)
(def ZMQ_RECONNECT_IVL 18)
(def ZMQ_BACKLOG 19)
(def ZMQ_RECONNECT_IVL_MAX 21)
(def ZMQ_MAXMSGSIZE 22)
(def ZMQ_SNDHWM 23)
(def ZMQ_RCVHWM 24)
(def ZMQ_MULTICAST_HOPS 25)
(def ZMQ_RCVTIMEO 27)
(def ZMQ_SNDTIMEO 28)
(def ZMQ_LAST_ENDPOINT 32)
(def ZMQ_ROUTER_MANDATORY 33)
(def ZMQ_TCP_KEEPALIVE 34)
(def ZMQ_TCP_KEEPALIVE_CNT 35)
(def ZMQ_TCP_KEEPALIVE_IDLE 36)
(def ZMQ_TCP_KEEPALIVE_INTVL 37)
(def ZMQ_IMMEDIATE 39)
(def ZMQ_XPUB_VERBOSE 40)
(def ZMQ_ROUTER_RAW 41)
(def ZMQ_IPV6 42)
(def ZMQ_MECHANISM 43)
(def ZMQ_PLAIN_SERVER 44)
(def ZMQ_PLAIN_USERNAME 45)
(def ZMQ_PLAIN_PASSWORD 46)
(def ZMQ_CURVE_SERVER 47)
(def ZMQ_CURVE_PUBLICKEY 48)
(def ZMQ_CURVE_SECRETKEY 49)
(def ZMQ_CURVE_SERVERKEY 50)
(def ZMQ_PROBE_ROUTER 51)
(def ZMQ_REQ_CORRELATE 52)
(def ZMQ_REQ_RELAXED 53)
(def ZMQ_CONFLATE 54)
(def ZMQ_ZAP_DOMAIN 55)
(def ZMQ_ROUTER_HANDOVER 56)
(def ZMQ_TOS 57)
(def ZMQ_CONNECT_ROUTING_ID 61)
(def ZMQ_GSSAPI_SERVER 62)
(def ZMQ_GSSAPI_PRINCIPAL 63)
(def ZMQ_GSSAPI_SERVICE_PRINCIPAL 64)
(def ZMQ_GSSAPI_PLAINTEXT 65)
(def ZMQ_HANDSHAKE_IVL 66)
(def ZMQ_SOCKS_PROXY 68)
(def ZMQ_XPUB_NODROP 69)
(def ZMQ_BLOCKY 70)
(def ZMQ_XPUB_MANUAL 71)
(def ZMQ_XPUB_WELCOME_MSG 72)
(def ZMQ_STREAM_NOTIFY 73)
(def ZMQ_INVERT_MATCHING 74)
(def ZMQ_HEARTBEAT_IVL 75)
(def ZMQ_HEARTBEAT_TTL 76)
(def ZMQ_HEARTBEAT_TIMEOUT 77)
(def ZMQ_XPUB_VERBOSER 78)
(def ZMQ_CONNECT_TIMEOUT 79)
(def ZMQ_TCP_MAXRT 80)
(def ZMQ_THREAD_SAFE 81)
(def ZMQ_MULTICAST_MAXTPDU 84)
(def ZMQ_VMCI_BUFFER_SIZE 85)
(def ZMQ_VMCI_BUFFER_MIN_SIZE 86)
(def ZMQ_VMCI_BUFFER_MAX_SIZE 87)
(def ZMQ_VMCI_CONNECT_TIMEOUT 88)
(def ZMQ_USE_FD 89)
(def ZMQ_GSSAPI_PRINCIPAL_NAMETYPE 90)
(def ZMQ_GSSAPI_SERVICE_PRINCIPAL_NAMETYPE 91)
(def ZMQ_BINDTODEVICE 92)

# Message options
(def ZMQ_MORE 1)
(def ZMQ_SHARED 3)

# Send/recv options
(def ZMQ_DONTWAIT 1)
(def ZMQ_SNDMORE 2)

# Security mechanisms
(def ZMQ_NULL 0)
(def ZMQ_PLAIN 1)
(def ZMQ_CURVE 2)
(def ZMQ_GSSAPI 3)

# Context options
(def ZMQ_IO_THREADS 1)
(def ZMQ_MAX_SOCKETS 2)
(def ZMQ_SOCKET_LIMIT 3)
(def ZMQ_THREAD_PRIORITY 3)
(def ZMQ_THREAD_SCHED_POLICY 4)
(def ZMQ_MAX_MSGSZ 5)
(def ZMQ_MSG_T_SIZE 6)
(def ZMQ_THREAD_AFFINITY_CPU_ADD 7)
(def ZMQ_THREAD_AFFINITY_CPU_REMOVE 8)
(def ZMQ_THREAD_NAME_PREFIX 9)

# Poll status
(def ZMQ_POLLIN 1)
(def ZMQ_POLLOUT 2)
(def ZMQ_POLLERR 4)
(def ZMQ_POLLPRI 8)


(defn setsockopt [socket option value]
  (zmq-native/setsockopt (get socket :socket) option value))


(defn getsockopt [socket option]
  (zmq-native/getsockopt (get socket :socket) option))


(defn- update-events [socket]
  (put socket :state (getsockopt socket ZMQ_EVENTS)))


(defn- handle_send [socket]
  (let [wchan (get socket :write-chan)
        sock  (get socket :socket)
        state (get socket :state)]
    (loop [:while (and (not= (band ZMQ_POLLOUT state) 0) (> (ev/count wchan) 0))]
      (let [msg (ev/take wchan)]
          (zmq-native/send sock msg ZMQ_DONTWAIT))
      (update-events socket))))


(defn- handle_recv [socket]
  (loop [:while (not= (band ZMQ_POLLIN (get socket :state)) 0)]
    (let [msg (zmq-native/recv (get socket :socket) ZMQ_DONTWAIT)]
      (ev/give (get socket :read-chan) msg)
      (update-events socket))))


(defn- handle_commands [socket]
  (let [cchan (get socket :command-chan)
        wchan (get socket :write-chan)
        sock (get socket :socket)]
    (loop [:while (> (ev/count cchan) 0)]
      (let [[cmd payload] (ev/take cchan)]
        (case cmd
          :send (ev/give wchan payload)
          :close (do
                   (zmq-native/close sock)
                   (error :stop)))))))


(defn- io-func [socket]
    (forever
      (try
        (do
          (put socket :state (zmq-native/poll (get socket :socket)))
          (handle_commands socket)
          (handle_send socket)
          (handle_recv socket))
        ([err] (break)))))
        
    
(defn ctx_new []
  (zmq-native/ctx_new))


(defn- start-io-func [socket]
  (let [io-fiber (ev/call io-func socket)]
    (put socket :io-fiber io-fiber)))


(defn socket [ctx type]
  (let [s (zmq-native/socket ctx type)
        rchan (ev/chan 1)
        cchan (ev/chan 1)
        wchan (ev/chan 1)
        sock @{:socket s :read-chan rchan :command-chan cchan :write-chan wchan :state 0}]
    (start-io-func sock)
    sock))


(defn close [socket]
  (ev/give (get socket :command-chan) [:close nil]))


(defn ctx_term [ctx]
  (zmq-native/ctx_term ctx))


(defn bind [socket endpoint]
  (zmq-native/bind (get socket :socket) endpoint))


(defn connect [socket endpoint]
  (zmq-native/connect (get socket :socket) endpoint))


(defn send [socket msg]
  (ev/give (get socket :command-chan) [:send msg]))


(defn recv [socket]
  (ev/take (get socket :read-chan)))

