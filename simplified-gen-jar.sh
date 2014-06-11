#!/bin/sh

name=harvester-0.1-SNAPSHOT-allinone.jar

mvn package

mv ./target/$name ./out/jars

cd ./out

# Client

cd ./jars
jar -xf $name
rm -f $name

rm -rf ./META-INF

echo "Manifest-Version: 1.0" > manifest.txt

jar -cvfm europeana.jar manifest.txt .

find . \! -name 'europeana.jar' -delete


