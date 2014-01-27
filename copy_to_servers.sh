cd ~/code/velox; sbt assembly
ASSEMBLY=~/code/velox/target/scala-2.10/velox-assembly-0.1.jar
scp $ASSEMBLY velox-client:~/
scp $ASSEMBLY velox-server0:~/
scp $ASSEMBLY velox-server1:~/

