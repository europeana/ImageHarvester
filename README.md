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
mvn clean package install
```

The build process creates one big jar with all the shaded dependencies in target.

### Start the harvester system

*Start the master harvester*

```
java -Djava.library.path="./lib" -cp target/harvester-0.1-SNAPSHOT-allinone.jar eu.europeana.harvester.cluster.MasterMain src/main/resources/localhost-demo-conf/master.conf
``` 

*Start the slave harvester*

```
java -Djava.library.path="./lib" -cp target/harvester-0.1-SNAPSHOT-allinone.jar eu.europeana.harvester.cluster.SlaveMain src/main/resources/localhost-demo-conf/slave.conf
``` 

### Send a harvester job to the master from your java code

```
java -Djava.library.path="./lib" -cp target/harvester-0.1-SNAPSHOT-allinone.jar eu.europeana.harvester.performance.LinkCheckJobSmall src/main/resources/localhost-demo-conf/client.conf  
```