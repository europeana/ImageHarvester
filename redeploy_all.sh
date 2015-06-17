#!/bin/bash

#stop all slaves
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev2.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev3.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev4.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod1.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
# ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod2.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod3.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod4.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod5.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod6.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod7.crf.europeana.eu 'sudo /etc/init.d/harvester-slave stop'

# stop the master
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev1.crf.europeana.eu 'sudo /etc/init.d/harvester-master stop'
# copy jar to master server
scp -i id_rsa ./harvester.jar crfharvester@dev1.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
# start the master
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev1.crf.europeana.eu 'sudo /etc/init.d/harvester-master start'
#give it some time to load jobs and stabilze
sleep 5m

# copy jar to all slaves and start them in sequence

scp -i ./id_rsa ./harvester.jar crfharvester@dev2.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev2.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@dev3.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev3.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@dev4.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@dev4.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@prod1.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod1.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

# for now do nothing with prod2 as it runs the Mongo instance
#scp -i ./id_rsa ./harvester.jar crfharvester@prod2.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
#ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod2.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@prod3.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod3.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@prod4.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod4.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@prod5.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod5.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@prod6.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod6.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

scp -i ./id_rsa ./harvester.jar crfharvester@prod7.crf.europeana.eu:/home/crfharvester/harvester/harvester.jar
ssh -i ./id_rsa -o StrictHostKeyChecking=no crfharvester@prod7.crf.europeana.eu 'sudo /etc/init.d/harvester-slave start'

echo "Done!"


