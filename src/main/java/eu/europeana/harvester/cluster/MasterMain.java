package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.routing.FromConfig;
import com.mongodb.MongoClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.ActorStart;
import eu.europeana.harvester.cluster.master.ClusterMasterActor;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import org.joda.time.Duration;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;

class MasterMain {

    public static void main(String[] args) {
        String configFilePath;

        if(args.length == 0) {
            configFilePath = "master";
        } else {
            configFilePath = args[0];
        }

        final Config config = ConfigFactory.load(configFilePath);

        final ClusterMasterConfig clusterMasterConfig =
                new ClusterMasterConfig(Duration.millis(config.getInt("akka.cluster.jobsPollingInterval")),
                        Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval")));

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.log().info("Will start when 1 backend members in the cluster.");

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
        final ProcessingLimitsDao processingLimitsDao = new ProcessingLimitsDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final LinkCheckLimitsDao linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);

        final ActorRef router = system.actorOf(FromConfig.getInstance().props(), "nodeMasterRouter");

        Cluster.get(system).registerOnMemberUp(new Runnable() {
            @Override
            public void run() {
                system.log().info("Joined");

                ActorRef clusterMaster = system.actorOf(Props.create(ClusterMasterActor.class, clusterMasterConfig,
                        processingJobDao, processingLimitsDao, sourceDocumentProcessingStatisticsDao,
                        sourceDocumentReferenceDao, linkCheckLimitsDao, router), "clusterMaster");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                clusterMaster.tell(new ActorStart(), ActorRef.noSender());
            }
        });

    }
}
