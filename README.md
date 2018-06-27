# Flink Training

Some flink examples

To run a flink application (in Eclipse), follow these steps : 

* Right click on the project -> run as -> maven build (package or clean before) : In order to build the jar file in target directory
* Run a server with netcat listenning on a port : ```nc -l 9999```
* Run the flink application with the following comand : 
```
bin/flink run -c yourpackage.yourclass Flink.jar --host localhost --port 9999
```
* Look at the output from the taskexecutors : 
```
tail -f log/flink-*-taskexecutor-*.out
```


Might have some errors due to type erasure ... 