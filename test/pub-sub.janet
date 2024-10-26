(import zmq)

(def workers 5)
(def iterations 100)

(defn publisher [ctx]
  (let [signal-socket (zmq/socket ctx zmq/ZMQ_REP)
        publisher-socket (zmq/socket ctx zmq/ZMQ_PUB)]
    (zmq/bind signal-socket "inproc://pub-sub-sig")
    (zmq/bind publisher-socket "inproc://pub-sub-pub")
    (each x (range workers) (do
      (zmq/recv signal-socket)
      (zmq/send signal-socket "")
    (each x (range iterations) (zmq/send publisher-socket (string x)))))))
    
(defn worker [ctx]
  (let [signal-socket (zmq/socket ctx zmq/ZMQ_REQ)
        subscription-socket (zmq/socket ctx zmq/ZMQ_SUB)]
    (var count 0)
    (zmq/setsockopt subscription-socket zmq/ZMQ_SUBSCRIBE "")
    (zmq/connect subscription-socket "inproc://pub-sub-pub")
    (zmq/connect signal-socket "inproc://pub-sub-sig")
    (zmq/send signal-socket "")
    (zmq/recv signal-socket)
    (each x (range iterations) (do
      (zmq/recv subscription-socket)
      (++ count)))
    (assert (= count iterations))))
    
(defn pub-sub []
  (let [ctx (zmq/ctx_new)]
    (ev/call publisher ctx)
    (each x (range workers) (ev/call worker ctx))))

(pub-sub)
    