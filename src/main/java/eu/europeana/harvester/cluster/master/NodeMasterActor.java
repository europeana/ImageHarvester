package eu.europeana.harvester.cluster.master;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.DefaultResizer;
import akka.routing.RoundRobinPool;
import akka.routing.RoundRobinRouter;
import akka.routing.SmallestMailboxPool;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.slave.SlaveActor;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class NodeMasterActor extends UntypedActor{

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    /**
     * The routers reference. We send all the messages to the router and then it decides which slave will get it.
     */
    private ActorRef router;
    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    /**
     * An object which contains all the config information needed by this actor to start.
     */
    private final NodeMasterConfig nodeMasterConfig;

    /**
     * Reference to the cluster master actor.
     * We need this to send him back statistics about the download, error messages or any other type of message.
     */
    private ActorRef clusterMaster;

    /**
     * Reference to the ping master actor.
     * We need this to send him back statistics about the ping, error messages or any other type of message.
     */
    private ActorRef pingMaster;

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
                //SmallestMailboxRouter - your choice
                new RoundRobinPool(nodeMasterConfig.getNrOfSlaves())
                        .withResizer(resizer)
                        .withSupervisorStrategy(strategy)
                        .props(Props.create(SlaveActor.class, channelFactory, hashedWheelTimer,
                                httpRetrieveResponseFactory, nodeMasterConfig.getResponseType(),
                                nodeMasterConfig.getPathToSave())),
                "router");
    }
Set<String> set = new HashSet<String>();
    int messages = 0;
    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            router.tell(message, getSelf());
            clusterMaster = getSender();
            System.out.println(messages++);

        } else
        if(message instanceof StartedUrl) {
            log.info("From " + getSender() + " to master: " +
                    ((StartedUrl)message).getUrl() + " started.");
            log.info("ClusterMaster: " + clusterMaster);
            clusterMaster.tell(message, getSelf());
            set.add(getSender().toString());
            System.out.println("Number of actors: " + set.size());
        } else
        if(message instanceof DoneDownload) {
            log.info("From " + getSender() + " to master: " +
                    ((DoneDownload) message).getUrl() + " " +
                    ((DoneDownload) message).getHttpResponseContentSizeInBytes()/1024/1024 + " done.");
            log.info("ClusterMaster: " + clusterMaster);

            clusterMaster.tell(message, getSelf());
        } else
        if(message instanceof StartPing) {
            pingMaster = getSender();
            router.tell(message, getSelf());
        } else
        if(message instanceof DonePing) {
            log.info("From " + getSender() + " to master: " + ((DonePing)message).getIpAddress());
            pingMaster.tell(message, getSelf());
        }
    }

}
