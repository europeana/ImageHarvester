#!/bin/bash

#stop all slaves
# ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.143 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.146 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.144 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.137 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.139 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.140 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.141 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.142 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.138 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.145 'sudo /etc/init.d/harvester-slave stop'

# stop the master
# ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev1.crf.europeana.eu 'sudo /etc/init.d/harvester-master stop'
# copy jar to master server
# scp -i id_rsa ./harvester.jar crfharvester@dev1.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
# start the master
# ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev1.crf.europeana.eu 'sudo /etc/init.d/harvester-master start'
# give it some time to load jobs and stabilze
# sleep 5m

# copy jar to all slaves and start them in sequence

# scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.143:/home/crfharvester/harvester/harvester.jar
# ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.143 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.146:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.146 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.144:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.144 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.137:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.137 'sudo /etc/init.d/harvester-slave start'


scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.139:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.139 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.140:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.140 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.141:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.141 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.142:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.142 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.138:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.138 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@136.243.48.145:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@136.243.48.145 'sudo /etc/init.d/harvester-slave start'

echo "Done!"


