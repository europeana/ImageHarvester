#!/bin/sh

mvn -Dmaven.test.skip=true -U clean install package
cd ./harvester-persistence/
mvn -Dmaven.test.skip=true -U clean install package
cd ../crf_harvester_publisher/
mvn -Dmaven.test.skip=true -U clean install package

scp ./target/crf_harvester_publisher-0.1-SNAPSHOT-allinone.jar norbert@europeana8.busymachines.com:/home/norbert/publisher/publisher.jar
