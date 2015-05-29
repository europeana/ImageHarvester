package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.PingMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.CheckForTaskTimeout;
import eu.europeana.harvester.cluster.domain.messages.LoadJobs;
import eu.europeana.harvester.cluster.domain.messages.Monitor;
import eu.europeana.harvester.cluster.master.ClusterMasterActor;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Duration;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;


class Master {
    private static Logger LOG = LogManager.getLogger(Master.class.getName());

    private final String[] args;

    private ActorSystem system;

    private ActorRef clusterMaster;

    private ActorRef pingMaster;
    private static final MetricRegistry metrics = new MetricRegistry();


    public Master(String[] args) {
        this.args = args;
    }

    public void init() throws MalformedURLException {


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

        final Integer jobsPerIP = config.getInt("mongo.jobsPerIP");
        final Duration receiveTimeoutInterval = Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval"));
        final Integer responseTimeoutFromSlaveInMillis =
                config.getInt("default-limits.responseTimeoutFromSlaveInMillis");
        final Long maxTasksInMemory = config.getLong("mongo.maxTasksInMemory");

        final ClusterMasterConfig clusterMasterConfig = new ClusterMasterConfig(jobsPerIP, maxTasksInMemory,
                receiveTimeoutInterval, responseTimeoutFromSlaveInMillis, WriteConcern.NONE);

        final PingMasterConfig pingMasterConfig =
                new PingMasterConfig(config.getInt("ping.timePeriod"), config.getInt("ping.nrOfPings"),
                        Duration.millis(config.getInt("akka.cluster.receiveTimeoutInterval")),
                        config.getInt("ping.timeoutInterval"), WriteConcern.NONE);

        Slf4jReporter reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(org.slf4j.LoggerFactory.getLogger("metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(60, TimeUnit.SECONDS);

        Graphite graphite = new Graphite(new InetSocketAddress(config.getString("metrics.graphiteServer"),
                config.getInt("metrics.graphitePort")));
        GraphiteReporter reporter2 = GraphiteReporter.forRegistry(metrics)
                .prefixedWith(config.getString("metrics.masterID"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter2.start(1, TimeUnit.MINUTES);

        system = ActorSystem.create("ClusterSystem", config);

        final Integer taskBatchSize = config.getInt("default-limits.taskBatchSize");
        final Long defaultBandwidthLimitReadInBytesPerSec =
                config.getLong("default-limits.bandwidthLimitReadInBytesPerSec");
        final Long defaultMaxConcurrentConnectionsLimit =
                config.getLong("default-limits.maxConcurrentConnectionsLimit");
        final Integer connectionTimeoutInMillis =
                config.getInt("default-limits.connectionTimeoutInMillis");
        final Integer maxNrOfRedirects =
                config.getInt("default-limits.maxNrOfRedirects");
        final Integer minDistanceInMillisBetweenTwoRequest =
                config.getInt("default-limits.minDistanceInMillisBetweenTwoRequest");
        final Double minTasksPerIPPercentage =
                config.getDouble("default-limits.minTasksPerIPPercentage");

        final DefaultLimits defaultLimits = new DefaultLimits(taskBatchSize, defaultBandwidthLimitReadInBytesPerSec,
                defaultMaxConcurrentConnectionsLimit, minDistanceInMillisBetweenTwoRequest,
                connectionTimeoutInMillis, maxNrOfRedirects, minTasksPerIPPercentage);

        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient(config.getString("mongo.host"), config.getInt("mongo.port"));
            Morphia morphia = new Morphia();
            String dbName = config.getString("mongo.dbName");

            if(!config.getString("mongo.username").equals("")) {
                final DB db = mongo.getDB("admin");
                final Boolean auth = db.authenticate(config.getString("mongo.username"),
                        config.getString("mongo.password").toCharArray());
                if(!auth) {
                    LOG.error("Mongo auth error");
                    System.exit(-1);
                }
            }

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        final ProcessingJobDao processingJobDao = new ProcessingJobDaoImpl(datastore);
        final MachineResourceReferenceDao machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao =
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore);




        final Integer ipExceptionsMaxConcurrentConnectionsLimit = config.getInt("IPExceptions.maxConcurrentConnectionsLimit");
        final List<String> ips = config.getStringList("IPExceptions.ips");
        final List<String> ignoredIPs = config.getStringList("IPExceptions.ignoredIPs");
        final IPExceptions ipExceptions = new IPExceptions(ipExceptionsMaxConcurrentConnectionsLimit, ips, ignoredIPs);

        final Integer cleanupInterval = config.getInt("akka.cluster.cleanupInterval");

        clusterMaster = system.actorOf(Props.create(ClusterMasterActor.class,
                clusterMasterConfig, ipExceptions, processingJobDao, machineResourceReferenceDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao,
                sourceDocumentReferenceMetaInfoDao, defaultLimits,
                cleanupInterval, metrics ), "clusterMaster");

//        pingMaster = system.actorOf(Props.create(PingMasterActor.class, pingMasterConfig, router,
//                machineResourceReferenceDao, machineResourceReferenceStatDao), "pingMaster");
    }

    public void start() {
        clusterMaster.tell(new LoadJobs(), ActorRef.noSender());
        clusterMaster.tell(new Monitor(), ActorRef.noSender());
        clusterMaster.tell(new CheckForTaskTimeout(), ActorRef.noSender());

        //pingMaster.tell(new LookInDB(), ActorRef.noSender());
    }

    public ActorSystem getActorSystem() {
        return system;
    }

    public static void main(String[] args) throws MalformedURLException {
        final Master master = new Master(args);
        master.init();
        master.start();
    }
}
