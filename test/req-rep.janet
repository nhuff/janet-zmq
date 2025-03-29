(import zmq)

(defn- requestor [ctx]
  (let [sock (zmq/socket ctx zmq/ZMQ_REQ)]
    (zmq/connect sock "inproc://req-rep")
    (zmq/send sock ["hello" "foo"])
    (assert (deep= (zmq/recv sock) @["world"]))
    (zmq/close sock)))


(defn- responder [ctx]
  (let [sock (zmq/socket ctx zmq/ZMQ_REP)]
    (zmq/bind sock "inproc://req-rep")
    (assert (deep= (zmq/recv sock) @["hello" "foo"]))
    (zmq/send sock "world")
    (zmq/close sock)))


(defn simple-req-rep []
    (def ctx (zmq/ctx_new))
    (ev/gather (requestor ctx) (responder ctx))
    (zmq/ctx_term ctx))


(simple-req-rep)
