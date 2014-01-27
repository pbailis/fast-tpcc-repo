
#start servers - send to background
java -cp target/scala-2.10/velox-assembly-0.1.jar -Xmx512m \
   edu.berkeley.velox.server.KVServerMain \
   -p 8080 -f 9000 -c 127.0.0.1:8080,127.0.0.1:8081 -i 0 \
   &

java -cp target/scala-2.10/velox-assembly-0.1.jar -Xmx512m \
   edu.berkeley.velox.server.KVServerMain \
   -p 8081 -f 9001 -c 127.0.0.1:8080,127.0.0.1:8081 -i 1 &

sleep 8

#start client - keep in foreground
java -cp target/scala-2.10/velox-assembly-0.1.jar -Xmx512m \
  edu.berkeley.velox.examples.ExampleClient \
   -m 127.0.0.1:9000,127.0.0.1:9001
