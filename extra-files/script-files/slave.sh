#!/bin/bash
java -Xmx4g -Djava.library.path="/home/norbert/harvester/extra-files/lib" -Dlogback.configurationFile="/home/norbert/harvester/extra-files/logback_slave.xml" -cp ./harvester.jar eu.europeana.harvester.cluster.Slave /home/norbert/harvester/extra-files/slave.conf
