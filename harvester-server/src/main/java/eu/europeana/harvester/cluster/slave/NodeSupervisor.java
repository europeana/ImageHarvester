package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
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

import java.util.concurrent.TimeUnit;

/**
 * This actor sends feedback for each task and supervises the node master actor, if it failes than restarts it.
 */
public class NodeSupervisor extends UntypedActor {

        public static ActorRef createActor(final ActorSystem system,final Slave slave, final ActorRef masterSender,
                                       final NodeMasterConfig nodeMasterConfig, final MediaStorageClient mediaStorageClient, MetricRegistry metrics) {
        return system.actorOf(Props.create(NodeSupervisor.class, slave, masterSender, nodeMasterConfig,
                mediaStorageClient, metrics), "nodeSupervisor");

    }

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Reference to the cluster master actor.
     * We need this to send request for new tasks.
     */
    private final ActorRef masterSender;

    private final Slave slave;

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

    public NodeSupervisor(final Slave slave, final ActorRef masterSender,
                          final NodeMasterConfig nodeMasterConfig, final MediaStorageClient mediaStorageClient, MetricRegistry metrics) {
        LOG.info("NodeSupervisor constructor");

        this.slave = slave;
        this.masterSender = masterSender;
        this.nodeMasterConfig = nodeMasterConfig;
        this.mediaStorageClient = mediaStorageClient;
        this.missedHeartbeats = 0;
        this.metrics = metrics;

        this.memberups = 0;
    }

    @Override
    public void preStart() throws Exception {
        LOG.info("NodeSupervisor preStart");

        nodeMaster = NodeMasterActor.createActor(context(), masterSender, getSelf(),
                nodeMasterConfig,
                mediaStorageClient,
                metrics);

        context().watch(nodeMaster);

        final Cluster cluster = Cluster.get(getContext().system());
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                ClusterEvent.MemberEvent.class, ClusterEvent.UnreachableMember.class, AssociatedEvent.class);
        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(3,
                TimeUnit.MINUTES), getSelf(), new SendHearbeat(), getContext().system().dispatcher(), getSelf());

    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof BagOfTasks) {
            onBagOfTasksReceived((BagOfTasks) message);
            return;
        }
        if (message instanceof Terminated) {
            onTerminatedReceived((Terminated) message);
            return;
        }
        if (message instanceof SendHearbeat) {
            onSendHeartBeatReceived(message);
            return;
        }
        if (message instanceof SlaveHeartbeat) {
            onSlaveHeartBeatReceived();
            return;
        }
        if (message instanceof DisassociatedEvent) {
            onDissasociatedEventReceived((DisassociatedEvent) message);
            return;
        }

        if (message instanceof AssociatedEvent) {
            onAssociatedEventReceived();
            return ;
        }

        if (message instanceof ClusterEvent.MemberUp) {
            onMemberUpReceived((ClusterEvent.MemberUp) message);
            return ;
        }
        // Anything else
        nodeMaster.tell(message, getSender());
    }

    private void onMemberUpReceived(ClusterEvent.MemberUp message) {
        ClusterEvent.MemberUp mUp = message;
        LOG.info("Member is Up: {}", mUp.member());
        memberups++;
        if (memberups == 2)
            nodeMaster.tell(new RequestTasks(), getSelf());
    }

    private void onAssociatedEventReceived() {
        //nodeMaster.tell( new RequestTasks(), getSelf());
        LOG.info("Associated and requesting tasks");
    }

    private void onDissasociatedEventReceived(DisassociatedEvent message) throws Exception {
        final DisassociatedEvent disassociatedEvent = message;
        LOG.info("Member disassociated: {}", disassociatedEvent.remoteAddress());
        try {
            Thread.sleep(300000);

        } catch (InterruptedException e) {
            LOG.info("Interrupted");
        }

        slave.restart();
    }

    private void onSlaveHeartBeatReceived() {
        LOG.info("Received slave heartbeat");
        missedHeartbeats = 0;
    }

    private void onSendHeartBeatReceived(Object message) {
        // for safety, send a job request
        nodeMaster.tell(new RequestTasks(), getSelf());

        if (missedHeartbeats >= 3) {
            LOG.error("Slave doesn't responded to the heartbeat 3 consecutive times. It will be restarted.");
            missedHeartbeats = 0;

            getContext().system().stop(nodeMaster);
            restartNodeMaster();
        }
        nodeMaster.tell(message, getSelf());
        missedHeartbeats += 1;

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(3,
                TimeUnit.MINUTES), getSelf(), new SendHearbeat(), getContext().system().dispatcher(), getSelf());
    }

    private void onTerminatedReceived(Terminated message) {
        LOG.info("Restarting NodeMasterActor...");
        final Terminated t = message;
        if (t.getActor() == nodeMaster) {
            restartNodeMaster();
        }
    }

    private void onBagOfTasksReceived(BagOfTasks message) {
        final BagOfTasks bagOfTasks = message;

        for (final RetrieveUrl request : bagOfTasks.getTasks()) {
            final StartedTask startedTask = new StartedTask(request.getId());

            getSender().tell(startedTask, getSelf());
            nodeMaster.tell(new RetrieveUrlWithProcessingConfig(request, nodeMasterConfig.getPathToSave() + "/" + request.getJobId(), nodeMasterConfig.getSource()), getSender());
        }
    }

    private void restartNodeMaster() {
        LOG.info("NodeSupervisor: restarting nodeMasterActor");

        nodeMaster = NodeMasterActor.createActor(context(), masterSender, getSelf(),
                nodeMasterConfig,
                mediaStorageClient,
                metrics);
        context().watch(nodeMaster);
    }
}
