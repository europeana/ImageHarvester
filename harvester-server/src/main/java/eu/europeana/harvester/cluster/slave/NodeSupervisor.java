package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.remote.AssociatedEvent;
import akka.remote.DisassociatedEvent;
import com.codahale.metrics.MetricRegistry;
import eu.europeana.harvester.cluster.Slave;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.db.MediaStorageClient;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.concurrent.TimeUnit;

/**
 * This actor sends feedback for each task and supervises the node master actor, if it failes than restarts it.
 */
public class NodeSupervisor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

    /**
     * Reference to the cluster master actor.
     * We need this to send request for new tasks.
     */
    private final ActorRef masterSender;

    private final Slave slave;

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

    /**
     * This client is used to save the thumbnails in Mongo.
     */
    private final MediaStorageClient mediaStorageClient;

    /**
     * NodeSupervisor sends heartbeat messages to the slave which responds with the same message.
     * If 3 consecutive messages are missed than the slave is restarted.
     */
    private Integer missedHeartbeats;
    private int memberups;

    private final MetricRegistry metrics;



    public NodeSupervisor(final Slave slave, final ActorRef masterSender, final ChannelFactory channelFactory,
                          final NodeMasterConfig nodeMasterConfig, final MediaStorageClient mediaStorageClient, MetricRegistry metrics) {
        LOG.info("NodeSupervisor constructor");

        this.slave = slave;
        this.masterSender = masterSender;
        this.channelFactory = channelFactory;
        this.nodeMasterConfig = nodeMasterConfig;
        this.mediaStorageClient = mediaStorageClient;
        this.missedHeartbeats = 0;
        this.metrics = metrics;

        this.memberups = 0;


    }

    @Override
    public void preStart() throws Exception {
        LOG.info("NodeSupervisor preStart");

        nodeMaster = context().system().actorOf(Props.create(NodeMasterActor.class,
                masterSender, getSelf(), channelFactory, nodeMasterConfig, mediaStorageClient, hashedWheelTimer, metrics),
                "nodeMaster");
        context().watch(nodeMaster);

        final Cluster cluster = Cluster.get(getContext().system());
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                ClusterEvent.MemberEvent.class, ClusterEvent.UnreachableMember.class, AssociatedEvent.class);
        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(3,
                TimeUnit.MINUTES), getSelf(), new SendHearbeat(), getContext().system().dispatcher(), getSelf());

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof BagOfTasks) {
            final BagOfTasks bagOfTasks = (BagOfTasks) message;

            for(final RetrieveUrl request : bagOfTasks.getTasks()) {
                final StartedTask startedTask = new StartedTask(request.getId());

                getSender().tell(startedTask, getSelf());
                nodeMaster.tell(request, getSender());
            }

            return;
        }
        if (message instanceof Terminated) {
            LOG.info("Restarting NodeMasterActor...");
            final Terminated t = (Terminated) message;
            if (t.getActor() == nodeMaster) {
                restartNodeMaster();
            }

            return;
        }
        if (message instanceof SendHearbeat) {
            // for safety, send a job request
            nodeMaster.tell( new RequestTasks(), getSelf());

            if(missedHeartbeats >= 3) {
                LOG.error("Slave doesn't responded to the heartbeat 3 consecutive times. It will be restarted.");
                missedHeartbeats = 0;

                getContext().system().stop(nodeMaster);
                restartNodeMaster();
            }
            nodeMaster.tell(message, getSelf());
            missedHeartbeats += 1;

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(3,
                    TimeUnit.MINUTES), getSelf(), new SendHearbeat(), getContext().system().dispatcher(), getSelf());

            return;
        }
        if (message instanceof SlaveHeartbeat) {
            LOG.info("Received slave heartbeat");
            missedHeartbeats = 0;

            return;
        }
        if (message instanceof DisassociatedEvent) {
            final DisassociatedEvent disassociatedEvent = (DisassociatedEvent) message;
            LOG.info("Member disassociated: {}", disassociatedEvent.remoteAddress());
            try {
                Thread.sleep(300000);

            } catch ( InterruptedException e) {
                LOG.info("Interrupted");
            }

            slave.restart();
            return;
        }

        if (message instanceof AssociatedEvent) {
            //nodeMaster.tell( new RequestTasks(), getSelf());
            LOG.info("Associated and requesting tasks");
        }

        if (message instanceof ClusterEvent.MemberUp) {
            ClusterEvent.MemberUp mUp = (ClusterEvent.MemberUp) message;
            LOG.info("Member is Up: {}", mUp.member());
            memberups++;
            if ( memberups == 2 )
                nodeMaster.tell( new RequestTasks(), getSelf());

        }
        // Anything else
        nodeMaster.tell(message, getSender());
    }

    private void restartNodeMaster() {
        LOG.info("NodeSupervisor: restarting nodeMasterActor");

        hashedWheelTimer.stop();
        hashedWheelTimer = new HashedWheelTimer();
        nodeMaster = context().system().actorOf(Props.create(NodeMasterActor.class,
                masterSender, getSelf(), channelFactory, nodeMasterConfig,
                mediaStorageClient, hashedWheelTimer), "nodeMaster");
        context().watch(nodeMaster);
    }
}
