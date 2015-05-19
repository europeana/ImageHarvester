#!/bin/sh

mvn -Dmaven.test.skip=true -U clean install
cd ./harvester-persistence/
mvn -Dmaven.test.skip=true -U clean install
cd ../crf_harvester_publisher/
mvn -Dmaven.test.skip=true -U clean install

scp ./target/crf_harvester_publisher-0.1-SNAPSHOT-allinone.jar root@europeana8.busymachines.com:/root/publisher/publisher.jar
#scp ./target/crf_harvester_publisher-0.1-SNAPSHOT-allinone.jar root@146.48.85.124:/root/publisher/publisher.jar
