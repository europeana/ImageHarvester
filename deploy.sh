#!/bin/sh

./build.sh

scp ./harvester.jar norbert@europeana1.busymachines.com:/home/norbert/harvester/harvester.jar
scp ./harvester.jar norbert@europeana0.busymachines.com:/home/norbert/harvester/harvester.jar
scp ./harvester.jar norbert@europeana5.busymachines.com:/home/norbert/harvester/harvester.jar
scp ./harvester.jar norbert@europeana6.busymachines.com:/home/norbert/harvester/harvester.jar
scp ./harvester.jar norbert@europeana7.busymachines.com:/home/norbert/harvester/harvester.jar
scp ./harvester.jar norbert@europeana8.busymachines.com:/home/norbert/harvester/harvester.jar
