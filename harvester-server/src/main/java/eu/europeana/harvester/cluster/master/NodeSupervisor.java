package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import org.jboss.netty.channel.ChannelFactory;

/**
 * This actor sends feedback for each task and supervises the node master actor, if it failes than restarts it.
 */
public class NodeSupervisor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * An object which contains all the config information needed by this actor to start.
     */
    private final NodeMasterConfig nodeMasterConfig;

    /**
     * A reference to the nodeMaster actor. This actor decides which slave execute which task.
     */
    private ActorRef nodeMaster;

    public NodeSupervisor(final ChannelFactory channelFactory, final NodeMasterConfig nodeMasterConfig) {
        this.channelFactory = channelFactory;
        this.nodeMasterConfig = nodeMasterConfig;
    }

    @Override
    public void preStart() throws Exception {
        LOG.info("Started node supervisor");
        nodeMaster = context().system().actorOf(Props.create(NodeMasterActor.class, channelFactory, nodeMasterConfig),
                "nodeMaster");
        context().watch(nodeMaster);
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            final RetrieveUrl task = (RetrieveUrl) message;
            final StartedUrl startedUrl = new StartedUrl(task.getJobId(), task.getReferenceId());

            getSender().tell(startedUrl, getSelf());

            nodeMaster.tell(message, getSender());

            return;
        }
        if (message instanceof Terminated) {
            LOG.info("Restarting NodeMasterActor...");
            final Terminated t = (Terminated) message;
            if (t.getActor() == nodeMaster) {
                nodeMaster =  context().system().actorOf(
                        Props.create(NodeMasterActor.class, channelFactory, nodeMasterConfig), "nodeMaster");
                context().watch(nodeMaster);
            }

            return;
        }

        // Anything else
        nodeMaster.tell(message, getSender());
    }
}
