ImageHarvester
==============

Distributed version of UIM Linkchecker/Thumbler

## How to test the harvester

### Infrastructure prerequisites

*RabbitMQ running on localhost*

Also it make sense to use the admin UI to check topics/etc.

```
http://localhost:15672/
```

*The RabbitMQ credentials are set to*

* user=guest
* password=guest

*The RabbitMQ must have at least the following two queues/topics defined:*

* harvesterIn  
* harvesterOut

*MongoDB server running on localhost without authentication*

*The master.conf, slave.conf & client.conf contain the above settings* 

* they all use the localhost RabbitMQ & MongoDB

### Compile the harvester system

*Build the entire system in one big jar (dependencies included)*

```
./simplified-gen-jar.sh
```

The europeana.jar is in the folder "out"

### Start the harvester system

*Start the master harvester*

```
java -Djava.library.path="./lib" -jar ./out/europeana.jar eu.europeana.harvester.cluster.MasterMain src/main/resources/localhost-demo-conf/master.conf
``` 

*Start the slave harvester*

```
java -Djava.library.path="./lib" -jar ./out/europeana.jar eu.europeana.harvester.cluster.SlaveMain src/main/resources/localhost-demo-conf/slave.conf
``` 

### Send a harvester job to the master from your java code

Just run the test.Main class
