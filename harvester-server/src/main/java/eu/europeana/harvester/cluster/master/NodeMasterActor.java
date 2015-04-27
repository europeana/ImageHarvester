package eu.europeana.harvester.cluster.master;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.SmallestMailboxPool;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.utils.ActorState;
import eu.europeana.harvester.cluster.slave.DownloaderSlaveActor;
import eu.europeana.harvester.cluster.slave.PingerSlaveActor;
import eu.europeana.harvester.cluster.slave.ProcesserSlaveActor;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This actor decides which slave execute which task.
 */
public class NodeMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

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
     * Reference to the node supervisor actor.
     */
    private final ActorRef nodeSupervisor;

    /**
     * Reference to the receiver master actor.
     * We need this to send him back statistics about the download, error messages or any other type of message.
     */
    private ActorRef masterReceiver;

    /**
     * Reference to the cluster master actor.
     * We need this to send request for new tasks.
     */
    private ActorRef masterSender;

    /**
     * Reference to the ping master actor.
     * We need this to send him back statistics about the ping, error messages or any other type of message.
     */
    private ActorRef pingMaster;

    /**
     * List of unprocessed messages.
     */
    private final Queue<Object> messages = new LinkedList<>();

    /**
     * List of downloaderActors. (this is needed by our locally implemented router)
     */
    private final HashMap<ActorRef, ActorState> downloaderActors = new HashMap<>();

    /**
     * Router actor which sends messages to slaves which will do different operation on downloaded documents.
     */
    private ActorRef processerRouter;

    /**
     * Router actor which sends messages to slaves which will ping different servers
     * and create statistics on their response time.
     */
    private ActorRef pingerRouter;

    /**
     * List of jobs which was stopped by the clients.
     */
    private final Set<String> jobsToStop;

    /**
     * This client is used to save the thumbnails in Mongo.
     */
    private final MediaStorageClient mediaStorageClient;

    private Boolean sentRequest;

    private final MetricRegistry metrics;

    private Meter retrieve, doneDl, doneProc;

    public NodeMasterActor(final ActorRef masterSender, final  ActorRef nodeSupervisor,
                           final ChannelFactory channelFactory, final NodeMasterConfig nodeMasterConfig,
                           final MediaStorageClient mediaStorageClient, final HashedWheelTimer hashedWheelTimer, final MetricRegistry metrics ) {
        LOG.info("NodeMasterActor constructor");

        this.masterSender = masterSender;
        this.nodeSupervisor = nodeSupervisor;
        this.channelFactory = channelFactory;
        this.nodeMasterConfig = nodeMasterConfig;
        this.mediaStorageClient = mediaStorageClient;

        this.hashedWheelTimer = hashedWheelTimer;
        this.jobsToStop = new HashSet<>();

        this.sentRequest = false;
        this.metrics = metrics;
    }

    @Override
    public void preStart() throws Exception {
        LOG.info("NodeMasterActor preStart");

        retrieve = metrics.meter("RetrieveURL");
        doneDl = metrics.meter("DoneDownload");
        doneProc = metrics.meter("DoneProcessing");

        // Slaves for download
        final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
        final ExecutorService service = Executors.newCachedThreadPool();

        for(int i = 0; i < nodeMasterConfig.getNrOfDownloaderSlaves(); i++) {
            final ActorRef newActor = getContext().system().actorOf(Props.create(DownloaderSlaveActor.class,
                    channelFactory, hashedWheelTimer, httpRetrieveResponseFactory, nodeMasterConfig.getResponseType(),
                    nodeMasterConfig.getPathToSave(), service, metrics ));
            downloaderActors.put(newActor, ActorState.READY);
        }

        // Slaves for processing downloaded content
        final int maxNrOfRetries = nodeMasterConfig.getNrOfRetries();
        final SupervisorStrategy strategy =
                new OneForOneStrategy(maxNrOfRetries, scala.concurrent.duration.Duration.create(1, TimeUnit.MINUTES),
                        Collections.<Class<? extends Throwable>>singletonList(Exception.class));

        processerRouter = getContext().actorOf(
                new SmallestMailboxPool(nodeMasterConfig.getNrOfExtractorSlaves())
                        .withSupervisorStrategy(strategy)
                        .withDispatcher("processer-dispatcher")
                        .props(Props.create(ProcesserSlaveActor.class, nodeMasterConfig.getResponseType(),
                                mediaStorageClient, nodeMasterConfig.getSource(), nodeMasterConfig.getColorMapPath())),
                "processerRouter");

        // Slaves for pinging
        pingerRouter = getContext().actorOf(
                new SmallestMailboxPool(nodeMasterConfig.getNrOfPingerSlaves())
                        .withSupervisorStrategy(strategy)
                        .props(Props.create(PingerSlaveActor.class)),
                "pingerRouter");

        requestTasks();
        sendMessage();

        monitor();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            if(sentRequest) {
                sentRequest = false;
            }
            messages.add(message);

            masterReceiver = getSender();
            retrieve.mark();

            return;
        }
        if(message instanceof DoneDownload) {
            final DoneDownload doneDownload = (DoneDownload) message;
            final String jobId = doneDownload.getJobId();
            downloaderActors.put(getSender(), ActorState.READY);

            if(!jobsToStop.contains(jobId)) {
                masterReceiver.tell(new DownloadConfirmation(doneDownload.getTaskID(), doneDownload.getIpAddress()), getSelf());

                if((DocumentReferenceTaskType.CHECK_LINK).equals(doneDownload.getDocumentReferenceTask().getTaskType()) ||
                        (ProcessingState.ERROR).equals(doneDownload.getProcessingState())) {

                    masterReceiver.tell(new DoneProcessing(doneDownload, null, null, null, null), getSelf());
                    deleteFile(doneDownload.getReferenceId());
                } else {
                    processerRouter.tell(message, getSelf());
                }
            }
            doneDl.mark();

            return;
        }
        if(message instanceof DoneProcessing) {
            final DoneProcessing doneProcessing = (DoneProcessing)message;
            final String jobId = doneProcessing.getJobId();

            deleteFile(doneProcessing.getReferenceId());

            if(!jobsToStop.contains(jobId)) {
                masterReceiver.tell(message, getSelf());
                LOG.info("Slave sending Doneprocessing message for job {} and task {}", doneProcessing.getJobId(), doneProcessing.getTaskID());
            }

            doneProc.mark();

            return;
        }
        if(message instanceof StartPing) {
            pingMaster = getSender();

            pingerRouter.tell(message, getSelf());

            return;
        }
        if(message instanceof DonePing) {
            pingMaster.tell(message, getSelf());

            return;
        }
        if(message instanceof ChangeJobState) {
            final ChangeJobState changeJobState = (ChangeJobState) message;
            LOG.info("Changing job state to: {}", changeJobState.getNewState());
            switch (changeJobState.getNewState()) {
                case PAUSE:
                    jobsToStop.add(changeJobState.getJobId());
                    break;
                case RESUME:
                    jobsToStop.remove(changeJobState.getJobId());
                    break;
                case RUNNING:
                    jobsToStop.remove(changeJobState.getJobId());
            }

            return;
        }
        if(message instanceof SendHearbeat) {
            getSender().tell(new SlaveHeartbeat(), getSelf());

            return;
        }
        if(message instanceof Clean) {
            LOG.info("Cleaning up slave");

            hashedWheelTimer.stop();
            context().system().stop(getSelf());

            return;
        }
    }

    Long lastRequest;
    /**
     * Requests tasks if in the queue are only a few tasks.
     */
    private void requestTasks() {
        final TimerTask timerTask = new TimerTask() {
            public void run(final Timeout timeout) throws Exception {
                LOG.info("Request tasks.");
                if(!sentRequest && masterSender != null && messages.size() < nodeMasterConfig.getTaskNrLimit()) {
                    LOG.info("Sent request.");

                    masterSender.tell(new RequestTasks(), nodeSupervisor);
                    sentRequest = true;
                    lastRequest = System.currentTimeMillis();
                } else {
                    //LOG.info("No request: " + messages.size() + " " + sentRequest);
                    if(sentRequest && lastRequest != null) {
                        final Long currentTime = System.currentTimeMillis();
                        final Long diff = (currentTime - lastRequest) / 1000;
                        if(diff > 30) {
                            sentRequest = false;
                        }
                    }
                }
                hashedWheelTimer.newTimeout(this, 5, TimeUnit.SECONDS);
            }
        };

        hashedWheelTimer.newTimeout(timerTask, 5, TimeUnit.SECONDS);
    }

    /**
     * Sends check link or download task to free downloader actors.
     */
    private void sendMessage() {
        final TimerTask timerTask = new TimerTask() {
            public void run(final Timeout timeout) throws Exception {
                boolean readyActor = true;
                while(messages.size() != 0 && readyActor) {
                    readyActor = false;
                    for(final ActorRef actorRef : downloaderActors.keySet()) {
                        if(downloaderActors.get(actorRef).equals(ActorState.READY)) {
                            final Object message = messages.poll();
                            if(message != null) {
                                actorRef.tell(message, getSelf());
                                downloaderActors.put(actorRef, ActorState.BUSY);
                                readyActor = true;
                                break;
                            }
                        }
                    }
                }
                hashedWheelTimer.newTimeout(this, 1000, TimeUnit.MILLISECONDS);
            }
        };

        hashedWheelTimer.newTimeout(timerTask, 1000, TimeUnit.MILLISECONDS);
    }

    private void deleteFile(String fileName) {
        final String path = nodeMasterConfig.getPathToSave() + "/" + fileName;
        final File file = new File(path);
        if(file.exists()) {
            file.delete();
        }
    }

    /**
     * ONLY for debug
     */
    private void monitor() {
        final TimerTask timerTask = new TimerTask() {
            public void run(final Timeout timeout) throws Exception {
                int nr = 0;
                for(ActorRef actorRef : downloaderActors.keySet()) {
                    if((ActorState.BUSY).equals(downloaderActors.get(actorRef))) {
                        nr++;
                    }
                }
                LOG.info("Messages: {}", messages.size());
                LOG.info("All downloaderActors: {} working: {}", downloaderActors.size(), nr);
                hashedWheelTimer.newTimeout(this, 10000, TimeUnit.MILLISECONDS);
            }
        };
        hashedWheelTimer.newTimeout(timerTask, 10000, TimeUnit.MILLISECONDS);
    }

}
