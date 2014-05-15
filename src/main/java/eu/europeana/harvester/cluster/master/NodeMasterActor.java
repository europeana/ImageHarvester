package eu.europeana.harvester.cluster.master;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.DefaultResizer;
import akka.routing.SmallestMailboxPool;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.SendResponse;
import eu.europeana.harvester.cluster.domain.messages.StartedUrl;
import eu.europeana.harvester.cluster.slave.SlaveActor;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class NodeMasterActor extends UntypedActor{

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    private ActorRef router;
    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    private final NodeMasterConfig nodeMasterConfig;

    private ActorRef clusterMaster;

    public NodeMasterActor(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer,
                           NodeMasterConfig nodeMasterConfig) {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.nodeMasterConfig = nodeMasterConfig;
    }

    @Override
    public void preStart() throws Exception {
        final DefaultResizer resizer =
                new DefaultResizer(nodeMasterConfig.getMinNrOfSlaves(), nodeMasterConfig.getMaxNrOfSlaves());
        final int maxNrOfRetries = nodeMasterConfig.getNrOfRetries();
        final SupervisorStrategy strategy =
                new OneForOneStrategy(maxNrOfRetries, scala.concurrent.duration.Duration.create(1, TimeUnit.MINUTES),
                        Collections.<Class<? extends Throwable>>singletonList(Exception.class));

        final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
        router = getContext().actorOf(
                new SmallestMailboxPool(nodeMasterConfig.getNrOfSlaves())
                        .withResizer(resizer)
                        .withSupervisorStrategy(strategy)
                        .props(Props.create(SlaveActor.class, channelFactory, hashedWheelTimer,
                                httpRetrieveResponseFactory, nodeMasterConfig.getResponseType(),
                                nodeMasterConfig.getPathToSave())),
                "router");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            router.tell(message, getSelf());
            clusterMaster = getSender();
        } else
        if(message instanceof StartedUrl) {
            log.info("From " + getSender() + " to master: " +
                    ((StartedUrl)message).getUrl() + " started.");
            log.info("ClusterMaster: " + clusterMaster);
            clusterMaster.tell(message, getSelf());
        } else
        if(message instanceof SendResponse) {
            log.info("From " + getSender() + " to master: " +
                    ((SendResponse)message).getHttpRetrieveResponse().getUrl() + " " +
                    ((SendResponse)message).getHttpRetrieveResponse().getContentSizeInBytes() + " done.");
            log.info("ClusterMaster: " + clusterMaster);

            final HttpRetrieveResponse httpRetrieveResponse = ((SendResponse)message).getHttpRetrieveResponse();

            clusterMaster.tell(new DoneDownload(((SendResponse)message).getHttpRetrieveResponse().getUrl(),
                    ((SendResponse)message).getJobId(), httpRetrieveResponse.getHttpResponseCode(),
                    httpRetrieveResponse.getHttpResponseContentType(), httpRetrieveResponse.getContentSizeInBytes(),
                    httpRetrieveResponse.getRetrievalDurationInSecs(), httpRetrieveResponse.getCheckingDurationInSecs(),
                    httpRetrieveResponse.getSourceIp(), httpRetrieveResponse.getHttpResponseHeaders()), getSelf());
        }
    }
}
