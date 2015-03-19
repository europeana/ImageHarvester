package test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.master.NodeMasterActor;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AkkaTest {

    private static final Logger LOG = LogManager.getLogger(AkkaTest.class.getName());

    public static void main(String[] args) {
        final String configFilePath = "./extra-files/config-files/slave.conf";

        final File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        final ExecutorService bossPool = Executors.newCachedThreadPool();
        final ExecutorService workerPool = Executors.newCachedThreadPool();

        final ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        final ResponseType responseType;
        if(config.getString("slave.responseType").equals("diskStorage")) {
            responseType = ResponseType.DISK_STORAGE;
        } else {
            responseType = ResponseType.MEMORY_STORAGE;
        }

        final NodeMasterConfig nodeMasterConfig = new NodeMasterConfig(config.getInt("slave.nrOfDownloaderSlaves"),
                config.getInt("slave.nrOfExtractorSlaves"), config.getInt("slave.nrOfPingerSlaves"),
                config.getInt("slave.nrOfRetries"), 50, config.getString("slave.pathToSave"), responseType, "CRF harvester", "./colormap.png");

        final ActorSystem system = ActorSystem.create("ClusterSystem", config);
        final ActorRef router = system.actorOf(Props.create(NodeMasterActor.class, channelFactory, nodeMasterConfig)
                .withDispatcher("my-dispatcher"),
                "nodeMaster");

        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                100*1024l /* write */, 100*1024l /* read */, Duration.ZERO, 0*1024l, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 10000, 10);

        final String outputFileName = "outLinkCheck";
        final File links = new File("./harvester-client/src/main/resources/TestLinks/" + outputFileName);

        try {
            final BufferedReader br = new BufferedReader(new FileReader(links));

            String line; int i = 0;
            final GenericSubTaskConfiguration jobConfigs = new GenericSubTaskConfiguration(new ThumbnailConfig(50, 50));
            final List<ProcessingJobSubTask> subTaskList = new ArrayList<ProcessingJobSubTask>();
            subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null));
            subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, jobConfigs));

            while((line = br.readLine()) != null) {
                LOG.debug("Line: {}", i);

                router.tell(new RetrieveUrl(line, httpRetrieveConfig, "1", "" + i++, null,
                        new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, "", subTaskList), ""), ActorRef.noSender());
            }

            Timer timer = new Timer();

            LOG.debug(Thread.activeCount());

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
    }
}
