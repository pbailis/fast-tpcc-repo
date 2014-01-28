#!/usr/bin/env bash

CLIENT=ec2-54-201-189-43.us-west-2.compute.amazonaws.com
SERVER0=ec2-54-201-177-37.us-west-2.compute.amazonaws.com
SERVER1=ec2-54-201-134-113.us-west-2.compute.amazonaws.com

ssh velox-server0 "java -cp /home/ubuntu/velox-assembly-0.1.jar -Xmx20g \
   edu.berkeley.velox.server.VeloxServerMain \
   -p 8080 -f 9000 -c $SERVER0:8080,$SERVER1:8081 -i 0 -u $CLIENT:8085 -d 5" &

ssh velox-server1 "java -cp /home/ubuntu/velox-assembly-0.1.jar -Xmx20g \
   edu.berkeley.velox.server.VeloxServerMain \
   -p 8081 -f 9001 -c $SERVER0:8080,$SERVER1:8081  -i 1" &

ssh velox-client "java -cp /home/ubuntu/velox-assembly-0.1.jar -Xmx20g \
   edu.berkeley.velox.benchmark.ClientBenchmark \
   -m $SERVER0:8080,$SERVER1:9001" &
