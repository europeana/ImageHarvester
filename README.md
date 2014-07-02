ImageHarvester
==============

Distributed version of UIM Linkchecker/Thumbler

## How to test the harvester

### Infrastructure prerequisites

1. RabbitMQ running on localhost

Also it make sense to use the admin UI to check topics/etc.

```
http://localhost:15672/
```

2. The RabbitMQ credentials are set to :
* user=guest
* password=guest

3. The RabbitMQ must have at least the following two queues/topics defined:
* harvesterIn  
* harvesterOut

4. MongoDB server running on localhost without authentication

5. The master.conf, slave.conf & client.conf contain the above settings 
- they all use the localhost RabbitMQ & MongoDB

### Compile the harvester system

1. Build the entire system in one big jar (dependencies included)
```
./simplified-gen-jar.sh
```

The europeana.jar is in the folder "out"

### Start the harvester system

1. Start the master harvester

```
java -Djava.library.path="./lib" -jar ./out/europeana.jar eu.europeana.harvester.cluster.MasterMain src/main/resources/localhost-demo-conf/master.conf
``` 

2. Start the slave harvester

```
java -Djava.library.path="./lib" -jar ./out/europeana.jar eu.europeana.harvester.cluster.SlaveMain src/main/resources/localhost-demo-conf/slave.conf
``` 

### Send a harvester job to the master from your java code
Just run the test.Main class
