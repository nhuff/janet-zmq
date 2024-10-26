(import zmq)

(defn simple-req-rep []
    (def ctx (zmq/ctx_new))
    (def s (zmq/socket ctx zmq/ZMQ_REP))
    (def s2 (zmq/socket ctx zmq/ZMQ_REQ))

    (zmq/bind s "tcp://localhost:5555")
    (zmq/connect s2 "tcp://localhost:5555")

    (zmq/send s2 ["hello" "foo"])
    (assert (deep= (zmq/recv s) @["hello" "foo"]))
    (zmq/send s "world")
    (assert (deep= (zmq/recv s2) @["world"]))
    (zmq/close s)
    (zmq/close s2)
    (zmq/ctx_term ctx))


(defn ev-req [ctx]
  (let [sock (zmq/socket ctx zmq/ZMQ_REQ)]
    (zmq/connect sock "inproc://ev-req-rep")
    (while true
      (zmq/send sock "req")
      (assert (deep= (zmq/recv sock) @["rep"])))))


(defn ev-rep [ctx]
  (let [sock (zmq/socket ctx zmq/ZMQ_REP)]
    (zmq/bind sock "inproc://ev-req-rep")
    (while true
      (assert (deep= (zmq/recv sock) @["req"]))
      (zmq/send sock "rep"))))


(defn ev-req-rep []
  (let [ctx (zmq/ctx_new)
        fibers @[]]
    (array/push fibers (ev/call ev-rep ctx))
    (array/push fibers (ev/call ev-req ctx))

    (ev/sleep 0.1)
    (map (fn [x] (ev/cancel x :stop)) fibers)))
      

(simple-req-rep)
(ev-req-rep)