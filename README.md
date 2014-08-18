ImageHarvester
==============

Distributed version of UIM Linkchecker/Thumbler

## Architecture & components overview

The ImageHarvester has 4 components :

* core: the harvester client
* core: the harvester server (distributed in a cluster)
* external dependency: a shared MongoDB database
* external dependency: a shared MQ based event bus


### The harvester client

The harvester client is the component used by external systems to send commands and retrieve statistics from the harvester server.
It is just a thin wrapper that facilitates the interaction with the harvester server. 
The client communicates with the server two ways : 

* writting/reading to/from the shared MongoDB databse  
* receiving events on the MQ

### The harvester server

The harvester server is where the actual work happens : it receives jobs from the client and sends back an event when the job is finished. The harvester server runs on a cluster where exactly one node is the master and all the rest are slaves (ie. it's a single master architecture).

The components of the harvester server :

* the server master : sends tasks to the slaves and collects responses
* the server slave : receives tasks from the master and after execution sends back responses

#### The harvester server master

Periodically polls the MongoDB database ProcessingJob to load new jobs. Each job is composed of a list of tasks. 
Tasks can be simple things like "execute one link check on a URL" or a more complex operation like "download URL, extract meta info & thumbnails". A task always refers to a URL.

The server master uses a internal load balancer to evenly distribute the tasks to the slaves and expects a response
for each task. When the response is received (ie. sucess or failure) it persist it in the database and marks it
accordingly. When all the tasks inside the job have finished the job is marked as finished and the server master sends the
confirmation on the MQ that the job is done.

#### The harvester server slave

Each slave node waits for tasks from the master. When a task is received it's processed by one of the slave workers.
For each task received the slave will send back a response when the task is done (or has finished with an error).

In the current implementation the harvester slave does not read or write from MongoDB. The task received by the slave
has all the needed information inside, so the slave doesn't need to contact any external source of information to execute it.  


### The configuration files

The harvester has 3 configuration files : 
* the harvester client configuration 
* the harvester server master configuration
* the harvester server slave configuration

#### Slave config

akka {

    # The logging level: debug/info/error/...

    loglevel = "DEBUG"

    actor {

        # Enables cluster capabilities in your Akka system
        provider = "akka.cluster.ClusterActorRefProvider"
    }

    remote {
        log-remote-lifecycle-events = off
        # Your slaves IP address and port
        netty.tcp {
            hostname = "127.0.0.1"
            port = 2551
        }
    }

    cluster {
        # The role of the joining actor
        roles = [nodeSupervisor]

        # Initial contact points of the cluster.
        # The nodes to join automatically at startup
        seed-nodes = ["akka.tcp://ClusterSystem@localhost:2551",
            "akka.tcp://ClusterSystem@127.0.0.1:5555"]

        # After this period it will be marked as unreachable if it sends no message to the cluster
        auto-down-unreachable-after = 3600s
    }
}

my-dispatcher {

    type = Dispatcher

    # Which kind of ExecutorService to use for this dispatcher
    executor = "fork-join-executor"
    fork-join-executor {
        # Min number of threads to cap factor-based parallelism number to
        parallelism-min = 1

        # The parallelism factor is used to determine thread pool size using the
        # following formula: ceil(available processors * factor). Resulting size
        # is then bounded by the parallelism-min and parallelism-max values.
        parallelism-factor = 1.0

        # Max number of threads to cap factor-based parallelism number to
        parallelism-max = 2
    }

    # Throughput defines the number of messages that are processed in a batch
    # before the thread is returned to the pool.
    throughput = 100
}

*# System specific configs*

slave {

    # Number of actors which are downloading documents
    nrOfDownloaderSlaves = 300

    # Number of actors which are processing documents
    nrOfExtractorSlaves = 50

    # Number of actors which are pinging servers
    nrOfPingerSlaves = 50

    # The number of retries to reach a slave before restarting it.
    nrOfRetries = 5

    # The path where the documents are saved
    pathToSave = "/tmp/europeana"

    # You can choose different types of responses: diskStorage, memoryStorage, noStorage
    responseType = diskStorage
}



### Master config

akka {
    # The logging level: debug/info/error/...
    loglevel = "DEBUG"

    actor {

        # Enables cluster capabilities in your Akka system
        provider = "akka.cluster.ClusterActorRefProvider"

        # adaptive-router
        deployment {

            # A specific router (nodeSupervisorRouter)
            /nodeSupervisorRouter = {
                # You can choose between different types of adaptive routers. In this case it is a mixed one.
                router = adaptive-group
                # metrics-selector = heap
                # metrics-selector = load
                # metrics-selector = cpu
                metrics-selector = mix

                # the path to the routed actors
                routees.paths = ["/user/nodeSupervisor"]

                cluster {

                    enabled = on
                    use-role = nodeSupervisor
                    allow-local-routees = off
                }
            }
        }
    }

    remote {
        # logging level is still determined by the global logging level of the actor system:
        # for example debug level remoting events will be only logged if the system
        # is running with debug level logging.
        # Failures to deserialize received messages also fall under this flag.
        log-remote-lifecycle-events = off

        netty.tcp {
            # The hostname or ip to bind the remoting to
            hostname = "127.0.0.1"
            # The default remote server port clients should connect to
            port = 5555
        }
    }

    cluster {
        # the role of the joining actor
        roles = [clusterMaster]

        # Initial contact points of the cluster.
        # The nodes to join automatically at startup
        seed-nodes = ["akka.tcp://ClusterSystem@127.0.0.1:5555"]

        # How often to check for nodes marked as unreachable by the failure
        # detector
        unreachable-nodes-reaper-interval = 30s

        failure-detector {
            implementation-class = "akka.remote.PhiAccrualFailureDetector"

            # How often keep-alive heartbeat messages should be sent to each connection.
            heartbeat-interval = 5 s

            # Defines the failure detector threshold.
            # A low threshold is prone to generate many wrong suspicions but ensures
            # a quick detection in the event of a real crash. Conversely, a high
            # threshold generates fewer mistakes but needs more time to detect
            # actual crashes.
            threshold = 20.0

            # Number of potentially lost/delayed heartbeats that will be
            # accepted before considering it to be an anomaly.
            # This margin is important to be able to survive sudden, occasional,
            # pauses in heartbeat arrivals, due to for example garbage collect or
            # network drop.
            acceptable-heartbeat-pause = 60 s
        }

        # after this period it will be marked as unreachable if it sends no message to the cluster
        auto-down-unreachable-after = 60s

        # The interval in milliseconds after which the master is looking for a new job in the db.
        jobsPollingInterval = 5000

        # The interval in milliseconds after which the master is sending new tasks if it can.
        taskStartingInterval = 2000

        receiveTimeoutInterval = 600
    }
}

mongo {

    #The ip of the machine where the MongoDB is installed
    host = "95.85.40.228"

    #The port which is used by the MongoDB server
    port = 27017

    #The name of the MongoDB database
    dbName = "europeana_harvester"

    # The number of jobs which are taken by the cluster master at a time. This is used for pagination.
    maxJobsPerIteration = 10
}

*# Default limits in different tasks*

default-limits {

    # Bandwidth limit at download in byte per seconds per machine
    bandwidthLimitReadInBytesPerSec = 102400

    # Number of concurrent connections per machine
    maxConcurrentConnectionsLimit = 3

    # Number of milliseconds after which we interrupt the connection.
    connectionTimeoutInMillis = 30000

    # The maximum level of redirection per address
    maxNrOfRedirects = 10

    # The maximum responsetime in milliseconds of a slave node to master node after a given task.
    # If the master does not get any feedback it resend the task to an other slave.
    responseTimeoutFromSlaveInMillis = 10000
}

*# Configuration for the ping actors*

ping {

    # The time interval in milliseconds between each ping.
    timePeriod = 86400000

    # Number of pings per ip
    nrOfPings = 5

    # The interval in milliseconds after which the ping is interrupted
    timeoutInterval = 4000
}

*# RabbitMQ eventbus configuration*

eventbus {

    host = "127.0.0.1"
    username = "guest"
    password = "guest"
    incomingQueue = "harvesterIn"
    outgoingQueue = "harvesterOut"
}

*# Configuration for the thumbnailing*
thumbnail {

    # The final image width in pixels.
    width = 50

    # The final image height in pixels.
    height = 50
}

### Client config

mongo {

    #The ip of the machine where the MongoDB is installed
    host = "95.85.40.228"

    #The port which is used by the MongoDB server
    port = 27017

    #The name of the MongoDB database
    dbName = "europeana_harvester"
}



## How to test the eu.europeana.harvester

### Infrastructure prerequisites

*RabbitMQ running on localhost*

Also it make sense to use the admin UI to check topics/etc.

```
http://localhost:15672/
```

*The RabbitMQ credentials are set to*

* user=guest
* password=guest

*The RabbitMQ must have at least the following two queues/topics defined:*

* harvesterIn  
* harvesterOut

*MongoDB server running on localhost without authentication*

*The master.conf, slave.conf & client.conf contain the above settings* 

* they all use the localhost RabbitMQ & MongoDB

*JSVC from Apache Commons for daemonizing*

*You must have locally installed the MediaChecker for meta info extraction and corelib-utils for thumbnailing*

### Building and using the eu.europeana.harvester system

#### Build the master and the slave

*Build the entire system in one big jar (dependencies included)*

```
mvn -Dmaven.test.skip=true clean install package
```

```
cd ./harvester-persistence/
```

```
mvn -Dmaven.test.skip=true clean install package
```

```
cd ../harvester-server/
```

```
mvn -Dmaven.test.skip=true clean install package
```

```
cd ..
```

The build process creates one big jar with all the shaded dependencies in target. (in harvester-server)

#### Build the client

```
mvn -Dmaven.test.skip=true clean install package
```

```
cd ./harvester-persistence/
```

```
mvn -Dmaven.test.skip=true clean install package
```

```
cd ../harvester-client/
```

```
mvn -Dmaven.test.skip=true clean install package
```

```
cd ..
```


#### Using the compiled JAR's directly (recommanded for dev)

*Start the master eu.europeana.harvester*

```
java -Xmx512m -Djava.library.path="./lib" -cp ./harvester-server/target/harvester-server-0.1-SNAPSHOT-allinone.jar eu.europeana.harvester.cluster.Master ./extra-files/config-files/localhost-demo-conf/master.conf
``` 

*Start the slave eu.europeana.harvester*

```
java -Xmx512m -Djava.library.path="./lib" -cp ./harvester-server/target/harvester-server-0.1-SNAPSHOT-allinone.jar eu.europeana.harvester.cluster.Slave extra-files/config-files/localhost-demo-conf/slave.conf
``` 

#### Sending jobs with the client

You can choose from 4 predefined test cases: LinkCheck_50k, LinkCheck_500k, Download_50k, Download_500k; but you can create your own jobs also

```
java -cp ./harvester-client/target/harvester-client-0.1-SNAPSHOT-allinone.jar eu.europeana.harvester.performance.LinkCheck_50k ./extra-files/config-files/localhost-demo-conf/client.conf
```

### Installing & using the compiled JAR's as UNIX daemons (recommanded for prod)

#### Installing the slave

1. create the user europeana (or make sure it's there)
  ```
  useradd -m europeana
  ```
2. copy the europeana-slave ./extra-files/daemon-files to /etc/init.d

3. give the europeana user the permissions to run the slave
  ```
  sudo chmod 777 ./europeana-slave
  ```
4. create the folders for the slave
  ```
  mkdir /home; mkdir /home/europeana; mkdir /home/europeana/code
  ```
5. copy the lib folder from . to /home/europeana/code

6. copy the eu.europeana.harvester-0.1-SNAPSHOT-allinone.jar from ./harvester-server/target to /home/europeana/code

7. copy the slave.conf from ./extra-files/config-files to /home/europeana/code

8. adapt the ip and other configs to your preferences in slave.conf

9. adapt the paths to the jar, java_home, config file and the user in the /etc/init.d/europeana-slave


#### Using the slave


*To start the slave execute:*

```
sudo /etc/init.d/europeana-slave start
```

*To stop the slave execute:*

```
sudo /etc/init.d/europeana-slave stop
```

#### Installing the master

1. create the user europeana (or make sure it's there)
  ```
  useradd -m europeana
  ```
2. copy the europeana-master ./extra-files/daemon-files to /etc/init.d

3. give the europeana user the permissions to run the slave
  ```
  sudo chmod 777 ./europeana-master
  ```
4. create the folders for the slave
  ```
  mkdir /home; mkdir /home/europeana; mkdir /home/europeana/code
  ```
5. copy the lib folder from . to /home/europeana/code

6. copy the eu.europeana.harvester-0.1-SNAPSHOT-allinone.jar from ./harvester-server/target to /home/europeana/code

7. copy the master.conf from ./extra-files/config-files to /home/europeana/code

8. adapt the ip and other configs to your preferences in master.conf

9. adapt the paths to the jar, java_home, config file and the user in the /etc/init.d/europeana-slave


#### Using the master

*To start the master execute:*

```
sudo /etc/init.d/europeana-master start
```

*To start the master execute:*

```
sudo /etc/init.d/europeana-master stop
```


## How to upgrade the Media File Checker - how to add new types of metadata

You have to follow the next few steps if you want to add new types of metadata to the Media File Checker:

1. in the harvester-persistent module eu.europeana.harvester.domain package you have to create a new class which implements Serializable. This class has to contain all the info you need from a specific content type
2. in the harvester-server module, eu.europeana.harvester.cluster.slave package there is a class ProcesserSlaveActor where you have to modify the extract() method. You have to add a case to the switch where you have to extract the needed metadata fields and put them in an object of the new type
3. in the harvester-server module, eu.europeana.harvester.cluster.domain.messages package in the DoneProcessing class you have to add as a final field the previously created class
4. in the harvester-persistence module, eu.europeana.harvester.domain package you have to add in the SourceDocumentReferenceMetaInfo class as a final field the previously created class
5. finally in the harvester-server module, eu.europeana.harvester.cluster, ReceiverClusterActor saveMetaInfo() method you have to build a new SourceDocumentReferenceMetaInfo object from the DoneProcessing object.


## How to upgrade the Media File Checker - how to add new metadata fields to an existing type

You have to follow the next few steps if you want to add metadata fields to the Media File Checker:

1. choose the class you want to upgrade from harvester-persistent module eu.europeana.harvester.domain package (it ends with MetInfo, e.g.: ImageMetaInfo)
2. add the new fields as private final fields and create for them getters
3. in the harvester-server module, eu.europeana.harvester.cluster.slave package, ProcesserSlaveActor classes extract<ExistingType>MetaInfo() function (e.g.: extractImageMetaInfo) you have to map the fields of the returned object from MediaCheckers get<ExistingType>Info()  function with the previously created classes fields.



## How to write the produced thumbnail to an external storage

You can easily add new types of storage with few steps:

1. create a new class in the harvester-server modules eu.europeana.harvester.httpclient.response package which extends HttpRetrieveResponseBase and implements HttpRetrieveResponse
2. add the new types name to the ResponseType enum from eu.europeana.harvester.httpclient.response package, harvester-server module
3. implement the following functions: init, getAbsolutePath, getContent, addContent, getContentSizeInBytes, close
4. in the HttpRetrieveResponseFactory from eu.europeana.harvester.httpclient.response package, harvester-server module add your new type in the create function
5. in the slave.conf file change the slave.responseType field to the new type of storage and in the SlaveDaemon class (harvester-server module, eu.europeana.harvester.cluster package) add in the if/else part the new responsetype
