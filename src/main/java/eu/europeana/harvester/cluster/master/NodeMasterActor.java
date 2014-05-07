package eu.europeana.harvester.cluster.master;

import akka.actor.*;
import akka.routing.DefaultResizer;
import akka.routing.SmallestMailboxPool;
import eu.europeana.harvester.cluster.messages.DoneDownload;
import eu.europeana.harvester.cluster.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.messages.SendResponse;
import eu.europeana.harvester.cluster.messages.StartedUrl;
import eu.europeana.harvester.cluster.slave.SlaveActor;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class NodeMasterActor extends UntypedActor{

    private ActorRef router;
    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    private final int nrOfSlaves;

    private ActorRef clusterMaster;

    public NodeMasterActor(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer, int nrOfSlaves) {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.nrOfSlaves = nrOfSlaves;
    }

    @Override
    public void preStart() throws Exception {
        System.out.println("PreStart");
        DefaultResizer resizer = new DefaultResizer(3, 5);
        int maxNrOfRetries = 5;
        SupervisorStrategy strategy =
                new OneForOneStrategy(maxNrOfRetries, scala.concurrent.duration.Duration.create(1, TimeUnit.MINUTES),
                        Collections.<Class<? extends Throwable>>singletonList(Exception.class));

        router = getContext().actorOf(
                new SmallestMailboxPool(nrOfSlaves)
                        .withResizer(resizer)
                        .withSupervisorStrategy(strategy)
                        .props(Props.create(SlaveActor.class, channelFactory, hashedWheelTimer)),
                "router");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            router.tell(message, getSelf());
            clusterMaster = getSender();
        } else
        if(message instanceof StartedUrl) {
            System.out.println("From " + getSender() + " to master: " +
                    ((StartedUrl)message).getUrl() + " started.");
            System.out.println("ClusterMaster: " + clusterMaster);
            clusterMaster.tell(message, getSelf());
        } else
        if(message instanceof SendResponse) {
            System.out.println("From " + getSender() + " to master: " +
                    ((SendResponse)message).getHttpRetrieveResponse().getUrl() + " " +
                    ((SendResponse)message).getHttpRetrieveResponse().getContentSizeInBytes() + " done.");
            System.out.println("ClusterMaster: " + clusterMaster);
            clusterMaster.tell(new DoneDownload(((SendResponse)message).getHttpRetrieveResponse().getUrl()), getSelf());
        }
    }
}
