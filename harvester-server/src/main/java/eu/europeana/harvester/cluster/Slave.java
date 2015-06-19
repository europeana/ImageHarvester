package eu.europeana.harvester.cluster;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.FromConfig;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.slave.NodeSupervisor;
import eu.europeana.harvester.cluster.slave.SlaveMetrics;
import eu.europeana.harvester.cluster.slave.validator.ImageMagicValidator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.mongo.MediaStorageClientImpl;
import eu.europeana.harvester.db.swift.SwiftConfiguration;
import eu.europeana.harvester.db.swift.SwiftMediaStorageClientImpl;
import eu.europeana.harvester.domain.MediaStorageClientConfig;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jclouds.ContextBuilder;
import org.jclouds.openstack.swift.v1.SwiftApi;
import org.jclouds.openstack.swift.v1.features.ContainerApi;
import org.jclouds.openstack.swift.v1.features.ObjectApi;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Slave {

    private static final Logger LOG = LogManager.getLogger(Slave.class.getName());

    private final String[] args;

    private ActorSystem system;

    private static final String containerName = "swiftUnitTesting";
    private ObjectApi objectApi;
    private SwiftApi  swiftApi;
    private ContainerApi containerApi;



    public Slave(String[] args) {
        this.args = args;
    }

    public void init(Slave slave) throws Exception {
        allRequirementsAreMetOrThrowException();

//        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
//                .convertRatesTo(TimeUnit.SECONDS)
//                .convertDurationsTo(TimeUnit.MILLISECONDS)
//                .build();




        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./extra-files/config-files/slave.conf";
        } else {
            configFilePath = args[0];
        }

        final File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final ExecutorService bossPool = Executors.newCachedThreadPool();
        final ExecutorService workerPool = Executors.newCachedThreadPool();

        final ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        final ResponseType responseType;

        if("diskStorage".equals(config.getString("slave.responseType"))) {
            responseType = ResponseType.DISK_STORAGE;
        } else {
            responseType = ResponseType.MEMORY_STORAGE;
        }

        final String pathToSave = config.getString("slave.pathToSave");
        final File dir = new File(pathToSave);
        if(!dir.exists()) {
            dir.mkdirs();
        }
        final String source = config.getString("media-storage.source");
        final String colorMapPath = config.getString("slave.colorMap");

        final Integer nrOfDownloaderSlaves = config.getInt("slave.nrOfDownloaderSlaves");
        final Integer nrOfExtractorSlaves = config.getInt("slave.nrOfExtractorSlaves");
        final Integer nrOfPingerSlaves = config.getInt("slave.nrOfPingerSlaves");
        final Integer nrOfRetries = config.getInt("slave.nrOfRetries");
        final Integer taskNrLimit = config.getInt("slave.taskNrLimit");

        final NodeMasterConfig nodeMasterConfig = new NodeMasterConfig(nrOfDownloaderSlaves, nrOfExtractorSlaves,
                nrOfPingerSlaves, nrOfRetries, taskNrLimit, pathToSave, responseType, source, colorMapPath);

        final String mediaStorageHost = config.getString("media-storage.host");
        final Integer mediaStoragePort = config.getInt("media-storage.port");
        final String mediaStorageDBName = config.getString("media-storage.dbName");
        final String mediaStorageNameSpace = config.getString("media-storage.nameSpace");
        final String mediaStorageUsername = config.getString("media-storage.username");
        final String mediaStoragePassword = config.getString("media-storage.password");

        final MediaStorageClientConfig mediaStorageClientConfig =
                new MediaStorageClientConfig(mediaStorageHost, mediaStoragePort,
                        mediaStorageUsername, mediaStoragePassword, mediaStorageDBName, mediaStorageNameSpace);

        MediaStorageClient mediaStorageClient = null;
        try {
            mediaStorageClient = new MediaStorageClientImpl(mediaStorageClientConfig);
            SwiftConfiguration swiftConfiguration = new SwiftConfiguration("https://auth.hydranodes.de:5000/v2.0",
            "d35f3a21-cf35-48a0-a035-99bfc2252528.swift.tenant@a9s.eu",
                    "c9b9ddb5-4f64-4e08-9237-1d6848973ee1.swift.user@a9s.eu",
                    "78ae7i9XO3O7CcdkDa87", containerName, "hydranodes");
            mediaStorageClient = new SwiftMediaStorageClientImpl(swiftConfiguration);

            // why is this here ?

            swiftApi = ContextBuilder.newBuilder("openstack-swift")
                    .credentials(swiftConfiguration.getIdentity(), swiftConfiguration.getPassword())
                    .endpoint(swiftConfiguration.getAuthUrl()).buildApi(SwiftApi.class);

            containerApi = swiftApi.getContainerApi(swiftConfiguration.getRegionName());

            objectApi = swiftApi.getObjectApi(swiftConfiguration.getRegionName(), swiftConfiguration.getContainerName());
            containerApi.get(swiftConfiguration.getContainerName());

        } catch (Exception e) {
            LOG.error("Error: connection failed to media-storage " + e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }

        Slf4jReporter reporter = Slf4jReporter.forRegistry(SlaveMetrics.METRIC_REGISTRY)
                .outputTo(org.slf4j.LoggerFactory.getLogger("metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();

        reporter.start(60, TimeUnit.SECONDS);

        Graphite graphite = new Graphite(new InetSocketAddress(config.getString("metrics.graphiteServer"),
                config.getInt("metrics.graphitePort")));
        GraphiteReporter reporter2 = GraphiteReporter.forRegistry(SlaveMetrics.METRIC_REGISTRY)
                .prefixedWith(config.getString("metrics.slaveID"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter2.start(1, TimeUnit.MINUTES);




        system = ActorSystem.create("ClusterSystem", config);

        final ActorRef masterSender = system.actorOf(FromConfig.getInstance().props(), "masterSender");

        NodeSupervisor.createActor(system, slave, masterSender, nodeMasterConfig,
                mediaStorageClient, SlaveMetrics.METRIC_REGISTRY);

        //system.actorOf(Props.create(MetricsListener.class), "metricsListener");
    }

    public void start() {

    }

    public void restart() throws Exception {
        system.shutdown();
        //sleep 10 minutes
        try {
            Thread.sleep(600000l);
        } catch ( InterruptedException e) {
            LOG.error(e.getMessage());
        }

        LOG.info("trying to restart the actor system.");

        this.init(this);
        this.start();
    }

    public ActorSystem getActorSystem() {
        return system;
    }

    public static void allRequirementsAreMetOrThrowException() throws Exception {
        new ImageMagicValidator("ImageMagick 6.9.0").doNothingOrThrowException();
        LOG.info("ImageMagic version 6.9.0 installed in the system. External dependency OK. Continuing");
    }

    public static void main(String[] args) throws Exception {
        final Slave slave = new Slave(args);
        slave.init(slave);
        slave.start();
    }

}
