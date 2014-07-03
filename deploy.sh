#!/bin/sh
mvn clean install package

ssh root@95.85.40.228 "/home/europeana/code/stop.sh"
ssh root@80.240.134.53 "/home/europeana/code/stop.sh"
ssh root@80.240.134.54 "/home/europeana/code/stop.sh"
ssh root@80.240.134.59 "/home/europeana/code/stop.sh"

scp ./target/harvester-0.1-SNAPSHOT-allinone.jar root@95.85.40.228:/home/europeana/code/europeana.jar
scp ./target/harvester-0.1-SNAPSHOT-allinone.jar root@80.240.134.53:/home/europeana/code/europeana.jar
scp ./target/harvester-0.1-SNAPSHOT-allinone.jar root@80.240.134.54:/home/europeana/code/europeana.jar
scp ./target/harvester-0.1-SNAPSHOT-allinone.jar root@80.240.134.59:/home/europeana/code/europeana.jar

