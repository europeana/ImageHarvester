package harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.JobConfigs;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.ThumbnailConfig;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.master.NodeMasterActor;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.ResponseType;
import junit.framework.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;
import org.junit.Test;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NodeMasterActorTest extends TestCase {

    private static final Logger LOG = LogManager.getLogger(NodeMasterActorTest.class.getName());

    private ActorRef router = null;

    public void setUp() throws Exception {
        String configFilePath = "./extra-files/config-files/slave.conf";

        File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
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
        final File dir = new File(pathToSave);
        if(!dir.exists()) {
            dir.mkdirs();
        }

        final NodeMasterConfig nodeMasterConfig = new NodeMasterConfig(config.getInt("slave.nrOfDownloaderSlaves"),
                config.getInt("slave.nrOfExtractorSlaves"), config.getInt("slave.nrOfPingerSlaves"),
                config.getInt("slave.nrOfRetries"), pathToSave, responseType);

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        router = system.actorOf(Props.create(NodeMasterActor.class, channelFactory, nodeMasterConfig),
                "nodeMaster");
    }

    @Test
    public void testOnReceive() throws Exception {
        LOG.debug("onrecive");
        HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                25*1024l /* write */, 25*1024l /* read */, Duration.ZERO, 0*1024l, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 10000, 10);

        final String outputFileName = "outLinkCheck";
        final File links = new File("./harvester-client/src/main/resources/TestLinks/" + outputFileName);

        while(router == null) {
            Thread.sleep(1000);
            LOG.debug("WAITING");
        }
LOG.debug("Start");
        try {
            BufferedReader br = new BufferedReader(new FileReader(links));

            String line = ""; int i = 0;
            JobConfigs jobConfigs = new JobConfigs(new ThumbnailConfig(50, 50));
            while((line = br.readLine()) != null) {
                LOG.debug(line);
                router.tell(new RetrieveUrl(line, httpRetrieveConfig, "1", "1", null,
                        DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, jobConfigs), ActorRef.noSender());
            }

            Timer timer = new Timer();

            timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    LOG.debug("?");

                }
            }, 600000, 500);


        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
        LOG.debug("END");
    }
}
