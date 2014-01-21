java -jar target/scala-2.10/velox-assembly-0.1.jar -p 8080 -c 127.0.0.1:8080,127.0.0.1:8081 -i 0 &
java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -jar target/scala-2.10/velox-assembly-0.1.jar -p 8081 -c 127.0.0.1:8080,127.0.0.1:8081 -i 1
kill `ps -Af | grep java | grep velox | cut -f 4 -d ' '`
kill `ps -Af | grep java | grep velox | cut -f 4 -d ' '`
