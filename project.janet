(declare-project :name "zmq")
(declare-native
  :name "zmq"
  :source ["src/bindings.c"]
  :cflags ["-I/usr/local/include"]
  :lflags ["-L/usr/local/lib" "-lzmq"])
