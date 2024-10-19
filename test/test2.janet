(import zmq)

(def ctx (delay (zmq/ctx_new)))

(defn vtime []
  (while true
    (prin ".")
    (flush)
    (ev/sleep 1)))

(defn srv-f []
  (let [sock (zmq/socket (ctx) 4)]
    (zmq/bind sock "inproc://test2")
    (while true
      (pp (zmq/recv sock))
      (zmq/send sock "server"))))

(defn client-f []
  (let [sock (zmq/socket (ctx) 3)]
    (zmq/connect sock "inproc://test2")
    (while true
      (ev/sleep 0.1)
      (zmq/send sock "client")
      (pp (zmq/recv sock)))))

(defn main [& argv]
  (def fibers @[])
  (array/push fibers (ev/call vtime))
  (array/push fibers (ev/call srv-f))
  (array/push fibers (ev/call client-f))

  (pp fibers)
  (ev/sleep 1)
  (map (fn [x] (ev/cancel x :stop)) fibers)
  (os/exit 0))
