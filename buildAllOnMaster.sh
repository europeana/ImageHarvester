#!/bin/sh

echo "dummping all unsaved changes"
git checkout $(git ls-file -m)
echo "changeing branch to master"
git checkout master
#should be useless
git checkout $(git ls-file -m)
git pull

mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ./harvester-persistence/
mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ../media-storage-client/
mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ../harvester-job-creator/
mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ../harvester-server/
mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ../crf-migration/
mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ../crf_harvester_publisher/
mvn -Dmaven.test.skip=true clean install package -DskipTests
cd ../harvester-client/
mvn -Dmaven.test.skip=true clean install package -DskipTests

cd ..

mv ./harvester-server/target/harvester-server-0.1-SNAPSHOT-allinone.jar ./harvester.jar