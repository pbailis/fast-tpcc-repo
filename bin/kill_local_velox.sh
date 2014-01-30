ps -Af | grep velox-assembly | grep VeloxServer | grep java | cut -d ' ' -f 4 | xargs kill

