package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.ResponseType;
import junit.framework.TestCase;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NodeMasterActorTest extends TestCase {

    private ActorRef router = null;

    public void setUp() throws Exception {
        String configFilePath = "./src/main/resources/slave.conf";

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

        final NodeMasterConfig nodeMasterConfig = new NodeMasterConfig(config.getInt("slave.nrOfSubSlaves"),
                config.getInt("slave.minNrOfSubSlaves"), config.getInt("slave.maxNrOfSubSlaves"),
                config.getInt("slave.nrOfRetries"), config.getString("slave.pathToSave"), responseType);

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        router = system.actorOf(Props.create(NodeMasterActor.class, channelFactory, hashedWheelTimer, nodeMasterConfig)
                .withDispatcher("my-thread-pool-dispatcher"),
                "nodeMaster");
    }

    @Test
    public void testOnReceive() throws Exception {
        HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                25*1024l /* write */, 25*1024l /* read */, Duration.ZERO, 0*1024l, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 10000);

        final String outputFileName = "outLinkCheck";
        final File links = new File("./src/main/resources/TestLinks/" + outputFileName);

        while(router == null) {
            Thread.sleep(1000);
            System.out.println("WAITING");
        }

        try {
            BufferedReader br = new BufferedReader(new FileReader(links));

            String line = ""; int i = 0;

            while((line = br.readLine()) != null) {
                System.out.println(line);
                router.tell(new RetrieveUrl(line, httpRetrieveConfig, "1", "1", null,
                        DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD), ActorRef.noSender());
            }

            Timer timer = new Timer();

            System.out.println(Thread.activeCount());

            timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    System.out.println("?");

                }
            }, 600000, 500);


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
