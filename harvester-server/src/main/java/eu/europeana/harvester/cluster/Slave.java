package eu.europeana.harvester.cluster;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.master.NodeSupervisor;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Slave {

    private static final Logger LOG = LogManager.getLogger(Slave.class.getName());

    private final String[] args;

    private ActorSystem system;

    public Slave(String[] args) {
        this.args = args;
    }

    public void init() {
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

        if(config.getString("slave.responseType").equals("diskStorage")) {
            responseType = ResponseType.DISK_STORAGE;
        } else {
            responseType = ResponseType.MEMORY_STORAGE;
        }

        final String pathToSave = config.getString("slave.pathToSave");
        final File dir = new File(pathToSave);
        if(!dir.exists()) {
            dir.mkdirs();
        }

        final NodeMasterConfig nodeMasterConfig = new NodeMasterConfig(config.getInt("slave.nrOfDownloaderSlaves"),
                config.getInt("slave.nrOfExtractorSlaves"), config.getInt("slave.nrOfPingerSlaves"),
                config.getInt("slave.nrOfRetries"), pathToSave, responseType);

        system = ActorSystem.create("ClusterSystem", config);
        system.actorOf(Props.create(NodeSupervisor.class, channelFactory, nodeMasterConfig),
                "nodeSupervisor");

        //system.actorOf(Props.create(MetricsListener.class), "metricsListener");
    }

    public void start() {

    }

    public ActorSystem getActorSystem() {
        return system;
    }

    public static void main(String[] args) {
        final Slave slave = new Slave(args);
        slave.init();
        slave.start();
    }
}
