package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.SmallestMailboxPool;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import scala.Option;

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

    private boolean firstTime = true;

    private List<ActorRef> actors = new ArrayList<>() ;

    Long lastRequest;
    final int maxSlaves;


    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
    final ExecutorService service = Executors.newCachedThreadPool();


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
        this.firstTime = true;
        this.metrics = metrics;
        this.maxSlaves = nodeMasterConfig.getNrOfDownloaderSlaves();


    }

    @Override
    public void preStart() throws Exception {
        LOG.info("NodeMasterActor preStart");

        retrieve = metrics.meter("RetrieveURL");
        doneDl = metrics.meter("DoneDownload");
        doneProc = metrics.meter("DoneProcessing");
        lastRequest = 0l;
        sentRequest = false;


        final int maxNrOfRetries = nodeMasterConfig.getNrOfRetries();
        final SupervisorStrategy strategy =
                new OneForOneStrategy(maxNrOfRetries, scala.concurrent.duration.Duration.create(1, TimeUnit.MINUTES),
                        Collections.<Class<? extends Throwable>>singletonList(Exception.class));

        // Slaves for pinging
        pingerRouter = getContext().actorOf(
                new SmallestMailboxPool(nodeMasterConfig.getNrOfPingerSlaves())
                        .withSupervisorStrategy(strategy)
                        .props(Props.create(PingerSlaveActor.class)),
                "pingerRouter");

        //requestTasks();
        //sendMessage();

        //self().tell(new RequestTasks(), ActorRef.noSender());

        //monitor();
    }



    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        LOG.info("NodeMasterActor preRestart");


        getContext().system().stop(pingerRouter);


    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        LOG.info("NodeMasterActor postRestart");

        sentRequest = false;
        lastRequest = 0l;

        self().tell(new RequestTasks(), ActorRef.noSender());

        //monitor();


    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof RetrieveUrl) {

            messages.add(message);

            masterReceiver = getSender();

            retrieve.mark();


            if ( actors.size()<maxSlaves & messages.size()>maxSlaves ) {

                for ( int i=0;i<maxSlaves;i++){

                    Object msg = null;

                    if (!messages.isEmpty())
                        msg = messages.poll();


                    if(msg != null) {

                        ActorRef newActor = getContext().system().actorOf(Props.create(ProcessorActor.class,
                                channelFactory, hashedWheelTimer, httpRetrieveResponseFactory, nodeMasterConfig.getResponseType(),
                                nodeMasterConfig.getPathToSave(), service,  mediaStorageClient,
                                nodeMasterConfig.getSource(), nodeMasterConfig.getColorMapPath(),metrics ));
                        actors.add(newActor);

                        context().watch(newActor);

                        newActor.tell(msg, getSelf());
                    }

                }

                LOG.info("Messages: {}", messages.size());
                LOG.info("All ProcessorActors: {} ", actors.size());
            }

            return;
        }

        if(message instanceof RequestTasks ) {
            //allways request tasks if the message is coming from the supervisor
            if ( getSender().equals(nodeSupervisor)) {
                if ( masterSender!= null && messages.size() < nodeMasterConfig.getTaskNrLimit() ) {
                    masterSender.tell(new RequestTasks(), nodeSupervisor);
                    sentRequest = true;
                    lastRequest = System.currentTimeMillis();
                    LOG.info("requesting tasks from nodesupervisor");
                }
            }
            else if(!sentRequest && masterSender != null && messages.size() < nodeMasterConfig.getTaskNrLimit()) {
                LOG.info("Sent request for tasks");

                masterSender.tell(new RequestTasks(), nodeSupervisor);
                sentRequest = true;
                lastRequest = System.currentTimeMillis();
            }
            else  {
                    final Long currentTime = System.currentTimeMillis();
                    final int diff = Math.round ( ((currentTime - lastRequest)/1000) );
                    if(diff > 10 && messages.size() < nodeMasterConfig.getTaskNrLimit() ) {
                        sentRequest = true;
                        //self().tell(new RequestTasks(), ActorRef.noSender());
                        masterSender.tell(new RequestTasks(), nodeSupervisor);
                        lastRequest = System.currentTimeMillis();
                        LOG.info("Requesting tasks time difference");
                    } else {
                        LOG.info("No request: " + messages.size() + " " + sentRequest + " task limits: "+
                                nodeMasterConfig.getTaskNrLimit()+" time diff : " +diff);

                    }

            }

        }

        if(message instanceof DoneDownload) {
            final DoneDownload doneDownload = (DoneDownload) message;
            final String jobId = doneDownload.getJobId();

            if(!jobsToStop.contains(jobId)) {

                masterReceiver.tell(new DownloadConfirmation(doneDownload.getTaskID(), doneDownload.getIpAddress()), getSelf());

                if((DocumentReferenceTaskType.CHECK_LINK).equals(doneDownload.getDocumentReferenceTask().getTaskType()) ||
                        (ProcessingState.ERROR).equals(doneDownload.getProcessingState())) {
                    LOG.info("Slave sending DoneDownload message for job {} and task {}", doneDownload.getJobId(), doneDownload.getTaskID());
                    masterReceiver.tell(new DoneProcessing(doneDownload, null, null, null, null), getSelf());
                    deleteFile(doneDownload.getReferenceId());

                }

            }
            doneDl.mark();

            return;
        }
        if(message instanceof DoneProcessing) {
            final DoneProcessing doneProcessing = (DoneProcessing)message;
            final String jobId = doneProcessing.getJobId();

            deleteFile(doneProcessing.getReferenceId());
            actors.remove(getSender());

            if(!jobsToStop.contains(jobId)) {
                masterReceiver.tell(message, getSelf());
                LOG.info("Slave sending DoneProcessing message for job {} and task {}", doneProcessing.getJobId(), doneProcessing.getTaskID());


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

        if ( message instanceof Terminated ) {
            final Terminated t = (Terminated) message;
            ActorRef which = t.getActor();
            actors.remove(which);
            LOG.info("Messages: {}", messages.size());
            LOG.info("All ProcessorActors: {} ", actors.size());
            LOG.info("Actor {} terminated, building another one",t.getActor());

            if (actors.size()<maxSlaves){
                Object msg = null;
                if (!messages.isEmpty())
                    msg = messages.poll();

                if(msg != null) {
                    ActorRef newActor = getContext().system().actorOf(Props.create(ProcessorActor.class,
                            channelFactory, hashedWheelTimer, httpRetrieveResponseFactory, nodeMasterConfig.getResponseType(),
                            nodeMasterConfig.getPathToSave(), service,  mediaStorageClient,
                            nodeMasterConfig.getSource(), nodeMasterConfig.getColorMapPath(),metrics ));
                    actors.add(newActor);
                    context().watch(newActor);
                    LOG.info("Built Actor {}, starting it",newActor);
                    newActor.tell(msg, getSelf());
                }
                if ( messages.size() < nodeMasterConfig.getTaskNrLimit()) {
                    self().tell(new RequestTasks(), ActorRef.noSender());
                }

            }
        }

    }




    private void deleteFile(String fileName) {
        final String path = nodeMasterConfig.getPathToSave() + "/" + fileName;
        final File file = new File(path);
        if(file.exists()) {
            file.delete();
        }
    }



}
