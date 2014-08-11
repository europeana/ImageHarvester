package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.routing.FromConfig;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.QueueingConsumer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.*;
import eu.europeana.harvester.cluster.domain.messages.CheckForTaskTimeout;
import eu.europeana.harvester.cluster.domain.messages.LookInDB;
import eu.europeana.harvester.cluster.domain.messages.RecoverAbandonedJobs;
import eu.europeana.harvester.cluster.domain.messages.StartTasks;
import eu.europeana.harvester.cluster.master.ClusterMasterActor;
import eu.europeana.harvester.cluster.master.PingMasterActor;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.servicebus.client.ESBClient;
import eu.europeana.servicebus.client.rabbitmq.RabbitMQClientAsync;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;

class Master {

    private static final Logger LOG = LogManager.getLogger(Master.class.getName());

    private final String[] args;

    private ActorSystem system;

    private ActorRef clusterMaster;

    private ActorRef pingMaster;

    public Master(String[] args) {
        this.args = args;
    }

    public void init() {
        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./extra-files/config-files/master.conf";
        } else {
            configFilePath = args[0];
        }

        File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final Duration jobsPollingInterval = Duration.millis(config.getInt("akka.cluster.jobsPollingInterval"));
        final Duration taskStartingInterval = Duration.millis(config.getInt("akka.cluster.taskStartingInterval"));
        final Integer maxJobsPerIteration = config.getInt("mongo.maxJobsPerIteration");
        final Duration receiveTimeoutInterval = Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval"));
        final Integer responseTimeoutFromSlaveInMillis =
                config.getInt("default-limits.responseTimeoutFromSlaveInMillis");

        final ClusterMasterConfig clusterMasterConfig =
                new ClusterMasterConfig(jobsPollingInterval, taskStartingInterval, maxJobsPerIteration,
                        receiveTimeoutInterval, responseTimeoutFromSlaveInMillis, WriteConcern.NONE);

        final PingMasterConfig pingMasterConfig =
                new PingMasterConfig(config.getInt("ping.timePeriod"), config.getInt("ping.nrOfPings"),
                        Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval")),
                        config.getInt("ping.timeoutInterval"), WriteConcern.NONE);

        system = ActorSystem.create("ClusterSystem", config);

        final Long defaultBandwidthLimitReadInBytesPerSec =
                config.getLong("default-limits.bandwidthLimitReadInBytesPerSec");
        final Long defaultMaxConcurrentConnectionsLimit =
                config.getLong("default-limits.maxConcurrentConnectionsLimit");
        final Integer connectionTimeoutInMillis =
                config.getInt("default-limits.connectionTimeoutInMillis");
        final Integer maxNrOfRedirects =
                config.getInt("default-limits.maxNrOfRedirects");

        final DefaultLimits defaultLimits =
                new DefaultLimits(defaultBandwidthLimitReadInBytesPerSec, defaultMaxConcurrentConnectionsLimit,
                        connectionTimeoutInMillis, maxNrOfRedirects);

        final Integer thumbnailWidth =
                config.getInt("thumbnail.width");
        final Integer thumbnailHeight =
                config.getInt("thumbnail.height");
        final ThumbnailConfig thumbnailConfig = new ThumbnailConfig(thumbnailWidth, thumbnailHeight);
        final JobConfigs jobConfigs = new JobConfigs(thumbnailConfig);

        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient(config.getString("mongo.host"), config.getInt("mongo.port"));
            Morphia morphia = new Morphia();
            String dbName = config.getString("mongo.dbName");

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        final ProcessingJobDao processingJobDao = new ProcessingJobDaoImpl(datastore);
        final MachineResourceReferenceDao machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        final MachineResourceReferenceStatDao machineResourceReferenceStatDao =
                new MachineResourceReferenceStatDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final LinkCheckLimitsDao linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);
        final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao =
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore);

        final ActorRef router = system.actorOf(FromConfig.getInstance().props(), "nodeSupervisorRouter");

        final String ebHost = config.getString("eventbus.host");
        final String ebUsername = config.getString("eventbus.username");
        final String ebPassword = config.getString("eventbus.password");
        final String ebIncomingQueue = config.getString("eventbus.incomingQueue");
        final String ebOutgoingQueue = config.getString("eventbus.outgoingQueue");

        ESBClient esbClientTemp = null;
        try {
            final Consumer consumer = new QueueingConsumer(null);
            esbClientTemp = new RabbitMQClientAsync(ebHost, ebIncomingQueue, ebOutgoingQueue, ebUsername, ebPassword, consumer);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        final ESBClient esbClient = esbClientTemp;

        clusterMaster = system.actorOf(Props.create(ClusterMasterActor.class, jobConfigs,
                clusterMasterConfig, processingJobDao, machineResourceReferenceDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao,
                sourceDocumentReferenceMetaInfoDao, linkCheckLimitsDao, router,
                defaultLimits, esbClient),
                "clusterMaster");

        pingMaster = system.actorOf(Props.create(PingMasterActor.class, pingMasterConfig, router,
                machineResourceReferenceDao, machineResourceReferenceStatDao), "pingMaster");
    }

    public void start() {
        clusterMaster.tell(new RecoverAbandonedJobs(), ActorRef.noSender());
        clusterMaster.tell(new LookInDB(), ActorRef.noSender());
        clusterMaster.tell(new StartTasks(), ActorRef.noSender());
        clusterMaster.tell(new CheckForTaskTimeout(), ActorRef.noSender());
        pingMaster.tell(new LookInDB(), ActorRef.noSender());
    }

    public ActorSystem getActorSystem() {
        return system;
    }

    public static void main(String[] args) {
        final Master master = new Master(args);
        master.init();
        master.start();
    }
}
