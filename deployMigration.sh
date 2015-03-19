#!/bin/sh

mvn -Dmaven.test.skip=true clean install package
cd ./harvester-persistence/
mvn -Dmaven.test.skip=true clean install package
cd ../harvester-client/
mvn -Dmaven.test.skip=true clean install package
cd ../crf-migration/
mvn -Dmaven.test.skip=true clean install package

scp ./target/crf-migration-0.1-SNAPSHOT-allinone.jar norbert@europeana1.busymachines.com:/home/norbert/migration/crf-migration.jar