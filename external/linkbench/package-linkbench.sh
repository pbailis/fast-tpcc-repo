cd ../scripts; ./install-velox-jar.sh; cd ../linkbench
mvn clean
mvn package -DskipTests=true


