package eu.europeana.harvester.cluster;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.master.NodeMasterActor;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class SlaveMain {

    public static void main(String[] args) {
        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./src/main/resources/slave.conf";
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

        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        ExecutorService bossPool = Executors.newCachedThreadPool();
        ExecutorService workerPool = Executors.newCachedThreadPool();

        ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        ResponseType responseType;
        if(config.getString("slave.responseType").equals("diskStorage")) {
            responseType = ResponseType.DISK_STORAGE;
        } else {
            responseType = ResponseType.MEMORY_STORAGE;
        }

        final String pathToSave = config.getString("slave.pathToSave");
        File dir = new File(pathToSave);
        if(!dir.exists()) {
            dir.mkdirs();
        }

        final NodeMasterConfig nodeMasterConfig = new NodeMasterConfig(config.getInt("slave.nrOfSubSlaves"),
                config.getInt("slave.minNrOfSubSlaves"), config.getInt("slave.maxNrOfSubSlaves"),
                config.getInt("slave.nrOfRetries"), pathToSave, responseType);

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        system.actorOf(Props.create(NodeMasterActor.class, channelFactory, hashedWheelTimer, nodeMasterConfig),
                "nodeMaster");

        //system.actorOf(Props.create(MetricsListener.class), "metricsListener");
    }
}
