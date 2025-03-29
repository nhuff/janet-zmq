(import zmq)

(def ctx (zmq/ctx_new))
(def req (zmq/socket ctx zmq/ZMQ_REQ))
(def rep (zmq/socket ctx zmq/ZMQ_REP))

(zmq/bind rep "tcp://localhost:5555")
(zmq/connect req "tcp://localhost:5555")
(zmq/send req "hello")
(ev/with-deadline 3 (pp (zmq/recv rep)))
(zmq/send rep "world")
(pp (zmq/recv req))
(print "bla")
