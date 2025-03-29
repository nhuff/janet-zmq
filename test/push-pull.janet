(import zmq)

(defn- pusher [ctx]
  (let [sock (zmq/socket ctx zmq/ZMQ_PUSH)]
    (zmq/bind sock "inproc://push-pull")
    (each i (range 1 101)
      (zmq/send sock (string i)))
    (zmq/close sock)))
    
(defn- puller [ctx]
  (let [sock (zmq/socket ctx zmq/ZMQ_PULL)]
    (zmq/connect sock "inproc://push-pull")
    (var res 0)
    (each i (range 1 101)
      (+= res (scan-number (0 (zmq/recv sock)))))
    (assert (= res 5050))
    (zmq/close sock)))

(defn push-pull []
  (def ctx (zmq/ctx_new))
  (ev/gather (pusher ctx) (puller ctx))
  (zmq/ctx_term ctx))

(push-pull)