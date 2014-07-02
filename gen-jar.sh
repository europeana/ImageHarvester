#!/bin/sh

name=harvester-0.1-SNAPSHOT-allinone.jar

mvn package

rm -Rf out
mkdir out

cp ./target/$name ./out/client
cp ./target/$name ./out/master
cp ./target/$name ./out/slave
mv ./target/$name ./out/slave

cd ./out

# Client

cd ./client
jar -xf $name
rm -f $name

rm -rf ./META-INF

echo "Manifest-Version: 1.0\nMain-Class: eu.europeana.harvester.client.ClientMain" > manifest.txt

jar -cvfm client.jar manifest.txt .

find . \! -name 'client.jar' -delete

cd ..

mv ./client/client.jar ./jars

# Master

cd ./master
jar -xf $name
rm -f $name

rm -rf ./META-INF

echo "Manifest-Version: 1.0\nMain-Class: eu.europeana.harvester.cluster.MasterMain" > manifest.txt

jar -cvfm master.jar manifest.txt .

find . \! -name 'master.jar' -delete

cd ..

mv ./master/master.jar ./jars

# Slave

cd ./slave
jar -xf $name
rm -f $name

rm -rf ./META-INF

echo "Manifest-Version: 1.0\nMain-Class: eu.europeana.harvester.cluster.SlaveMain" > manifest.txt

jar -cvfm slave.jar manifest.txt .

find . \! -name 'slave.jar' -delete

cd ..

mv ./slave/slave.jar ./jars

