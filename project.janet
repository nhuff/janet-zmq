(defn pkg-config [what]
  (def p (os/spawn ["pkg-config" ;what] :pe (merge {:out :pipe} (os/environ))))
  (:wait p)
  (unless (zero? (p :return-code))
    (error "pkg-config failed!"))
  (->>
    (:read (p :out) :all)
    (string/trim)
    (string/split " ")))

(declare-project :name "zmq")
(declare-native
  :name "zmq-native"
  :source ["src/bindings.c"]
  :cflags (pkg-config ["libzmq" "--cflags"])
  :lflags (pkg-config ["libzmq" "--libs"]))

(declare-source
  :prefix "zmq"
  :source ["src/init.janet"])