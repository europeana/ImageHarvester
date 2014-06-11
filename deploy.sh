./simplified-gen-jar.sh

ssh root@95.85.40.228 "/home/europeana/code/stop.sh"
ssh root@80.240.134.53 "/home/europeana/code/stop.sh"
ssh root@80.240.134.54 "/home/europeana/code/stop.sh"
ssh root@80.240.134.59 "/home/europeana/code/stop.sh"

scp ./out/jars/europeana.jar root@95.85.40.228:/home/europeana/code/
scp ./out/jars/europeana.jar root@80.240.134.53:/home/europeana/code/
scp ./out/jars/europeana.jar root@80.240.134.54:/home/europeana/code/
scp ./out/jars/europeana.jar root@80.240.134.59:/home/europeana/code/

