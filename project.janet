(declare-project :name "zmq")
(declare-native
  :name "zmq-native"
  :source ["src/bindings.c"]
  :cflags ["-I/usr/local/include"]
  :lflags ["-L/usr/local/lib" "-lzmq"])

(declare-source
  :prefix "zmq"
  :source ["src/init.janet"])