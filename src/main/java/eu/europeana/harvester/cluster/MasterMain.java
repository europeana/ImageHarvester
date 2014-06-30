package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.routing.FromConfig;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.PingMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.ActorStart;
import eu.europeana.harvester.cluster.master.ClusterMasterActor;
import eu.europeana.harvester.cluster.master.PingMasterActor;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.eventbus.EventService;
import eu.europeana.harvester.eventbus.subscribers.JobDoneSubscriber;
import org.joda.time.Duration;

import java.io.File;
import java.net.UnknownHostException;

class MasterMain {

    public static void main(String[] args) {
        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./src/main/resources/master.conf";
            //configFilePath = "./master.conf";
        } else {
            configFilePath = args[0];
        }

        File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            System.out.println("Config file not found!");
            System.exit(-1);
        }

        final Config config =
                ConfigFactory.parseFileAnySyntax(configFile,
                        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final ClusterMasterConfig clusterMasterConfig =
                new ClusterMasterConfig(Duration.millis(config.getInt("akka.cluster.jobsPollingInterval")),
                        Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval")), WriteConcern.NONE);

        final PingMasterConfig pingMasterConfig =
                new PingMasterConfig(Duration.millis(config.getInt("ping.timePeriod")), config.getInt("ping.nrOfPings"),
                        Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval")),
                        config.getInt("ping.timeoutInterval"), WriteConcern.NONE);

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.log().info("Will start when 1 backend members in the cluster.");

        final Long defaultBandwidthLimitReadInBytesPerSec =
                config.getLong("default-limits.bandwidthLimitReadInBytesPerSec");
        final Long defaultMaxConcurrentConnectionsLimit =
                config.getLong("default-limits.maxConcurrentConnectionsLimit");
        final Integer connectionTimeoutInMillis =
                config.getInt("default-limits.connectionTimeoutInMillis");

        final DefaultLimits defaultLimits =
                new DefaultLimits(defaultBandwidthLimitReadInBytesPerSec, defaultMaxConcurrentConnectionsLimit,
                        connectionTimeoutInMillis);

        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient(config.getString("mongo.host"), config.getInt("mongo.port"));
            Morphia morphia = new Morphia();
            String dbName = config.getString("mongo.dbName");

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
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

        final ActorRef router = system.actorOf(FromConfig.getInstance().props(), "nodeMasterRouter");

        final EventService eventService = new EventService();

        Cluster.get(system).registerOnMemberUp(new Runnable() {
            @Override
            public void run() {
                system.log().info("Joined");

                ActorRef clusterMaster = system.actorOf(Props.create(ClusterMasterActor.class, clusterMasterConfig,
                        processingJobDao, machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao,
                        sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao, linkCheckLimitsDao, router,
                        defaultLimits, eventService),
                        "clusterMaster");

                ActorRef pingMaster = system.actorOf(Props.create(PingMasterActor.class, pingMasterConfig, router,
                        machineResourceReferenceDao, machineResourceReferenceStatDao), "pingMaster");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                clusterMaster.tell(new ActorStart(), ActorRef.noSender());
                pingMaster.tell(new ActorStart(), ActorRef.noSender());
            }
        });

        JobDoneSubscriber jobDoneSubscriber = new JobDoneSubscriber(eventService);


    }
}
