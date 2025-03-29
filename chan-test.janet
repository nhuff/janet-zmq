(import zmq)

(defn chan-wrap [socket]
  (let [r-chan (ev/chan 1)
        w-chan (ev/chan 1)]
    (ev/call (fn [] (forever
                      (let [msg (zmq/recv socket)]
                        (prin "recv: ")
                        (pp msg)
                        (ev/give r-chan msg)))))
    (ev/call (fn [] (forever
                      (let [msg (ev/take w-chan)]
                        (prin "send: ")
                        (pp msg)
                        (zmq/send socket msg)))))
    [r-chan w-chan]))

(def ctx (zmq/ctx_new))
(def req-sock (zmq/socket ctx zmq/ZMQ_REQ))
(def rep-sock (zmq/socket ctx zmq/ZMQ_REP))
(zmq/bind rep-sock "inproc://chan-test")
(zmq/connect req-sock "inproc://chan-test")

(def req-chans (chan-wrap req-sock))
(def rep-chans (chan-wrap rep-sock))

(let [[req-read req-write] req-chans
      [rep-read rep-write] rep-chans]
  (ev/give req-write "hello")
  (ev/take rep-read)
  (ev/give rep-write "world")
  (ev/take req-read))