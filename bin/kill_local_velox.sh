ps -Af | grep velox-assembly | grep Velox | grep java | cut -d ' ' -f 4 | xargs kill

