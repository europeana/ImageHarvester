#!/bin/sh

mvn -Dmaven.test.skip=true -U clean install || { exit 1; }

cd ./harvester-persistence/
mvn -Dmaven.test.skip=true -U clean install|| { exit 1; }

cd ../harvester-server/
mvn -Dmaven.test.skip=true -U clean install || { exit 1; }

cd ../crf_harvester_publisher/
mvn -Dmaven.test.skip=true -U clean install || { exit 1; }

echo "copying files to the server"
sshpass -p 'XPv7Jw4chefcYN' scp ./target/crf_harvester_publisher-0.1-SNAPSHOT-allinone.jar root@78.46.164.244:/home/crfharvester/publisher/publisher.jar
echo "done copying to server"

echo "copying to second server"
sshpass -p 'BLwKQGwWKHeXVS' scp ./target/crf_harvester_publisher-0.1-SNAPSHOT-allinone.jar root@78.46.106.38:/home/crfharvester/publisher/publisher.jar
echo "done copying to second server"

echo "copying files to local /opt/publisher"
cp ./target/crf_harvester_publisher-0.1-SNAPSHOT-allinone.jar /opt/publisher/publisher.jar
echo "done copying to local"
