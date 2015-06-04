package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import scala.Option;

import java.io.File;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This acts as a load balancer for the "retrieve and process" actor.
 */
public class NodeMasterActor extends UntypedActor {

        public static ActorRef createActor(final ActorContext context, final ActorRef masterSender, final ActorRef nodeSupervisor,
                                           final NodeMasterConfig nodeMasterConfig,
                                           final MediaStorageClient mediaStorageClient){

        return context.system().actorOf(Props.create(NodeMasterActor.class,
                        masterSender, nodeSupervisor, nodeMasterConfig, mediaStorageClient),
                "nodeMaster");
    }

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

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
     * List of unprocessed messages.
     */
    private final Queue<Object> messages = new LinkedList<>();

    /**
     * List of jobs which was stopped by the clients.
     */
    private final Set<String> jobsToStop;

    private Boolean sentRequest;

    final private List<ActorRef> actors = new ArrayList<>() ;

    Long lastRequest;
    final int maxSlaves;

    private MediaStorageClient mediaStorageClient;

    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
    final ExecutorService service = Executors.newCachedThreadPool();

    public NodeMasterActor(final ActorRef masterSender, final  ActorRef nodeSupervisor,
                           final NodeMasterConfig nodeMasterConfig,
                           final MediaStorageClient mediaStorageClient
                           ) {
        LOG.info("NodeMasterActor constructor");

        this.masterSender = masterSender;
        this.nodeSupervisor = nodeSupervisor;
        this.nodeMasterConfig = nodeMasterConfig;

        this.jobsToStop = new HashSet<>();

        this.sentRequest = false;
        this.mediaStorageClient = mediaStorageClient;
        this.maxSlaves = nodeMasterConfig.getNrOfDownloaderSlaves();

    }

    @Override
    public void preStart() throws Exception {
        LOG.info("NodeMasterActor preStart");

        lastRequest = 0l;
        sentRequest = false;


        final int maxNrOfRetries = nodeMasterConfig.getNrOfRetries();
        final SupervisorStrategy strategy =
                new OneForOneStrategy(maxNrOfRetries, scala.concurrent.duration.Duration.create(1, TimeUnit.MINUTES),
                        Collections.<Class<? extends Throwable>>singletonList(Exception.class));
    }


    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        LOG.info("NodeMasterActor preRestart");


    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        LOG.info("NodeMasterActor postRestart");

        sentRequest = false;
        lastRequest = 0l;

        self().tell(new RequestTasks(), ActorRef.noSender());
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof RetrieveUrlWithProcessingConfig) {
            onRetrieveUrlWithProcessingConfigReceived(message);
            return;
        }

        if(message instanceof RequestTasks ) {
            onRequestTasksReceived();
            return;
        }

        if(message instanceof DoneDownload) {
            onDoneDownloadReceived((DoneDownload) message);
            return;
        }
        if(message instanceof DoneProcessing) {
            onDoneProcessingReceived(message);
            return;
        }
        if(message instanceof ChangeJobState) {
            onChangeJobStateReceived((ChangeJobState) message);
            return;
        }
        if(message instanceof SendHearbeat) {
            onSendHeartBeatReceived();
            return;
        }
        if(message instanceof Clean) {
            onCleanReceived();
            return;
        }

        if ( message instanceof Terminated ) {
            onTerminatedReceived((Terminated) message);
            return ;
        }

    }

    private void onTerminatedReceived(Terminated message) {
        final Terminated t = message;
        ActorRef which = t.getActor();
        removeActorFromReferenceArray(which);
        LOG.info("Messages: {}", messages.size());
        LOG.info("All ProcessorActors: {} ", actors.size());
        LOG.info("Actor {} terminated, building another one",t.getActor());

        if (actors.size()<maxSlaves){
            Object msg = null;
            if (!messages.isEmpty())
                msg = messages.poll();

            if(msg != null) {
                ActorRef newActor = RetrieveAndProcessActor.createActor(getContext().system(),
                        httpRetrieveResponseFactory, mediaStorageClient, nodeMasterConfig.getColorMapPath()
                        );
                addActorToReferenceArray(newActor);
                context().watch(newActor);
                LOG.info("Built Actor {}, starting it",newActor);
                newActor.tell(msg, getSelf());
            }
            if ( messages.size() < nodeMasterConfig.getTaskNrLimit()) {
                self().tell(new RequestTasks(), ActorRef.noSender());
            }

        }
    }

    private void onRetrieveUrlWithProcessingConfigReceived(Object message) {
        messages.add(message);

        masterReceiver = getSender();

        if ( actors.size()<maxSlaves & messages.size()>maxSlaves ) {

            for ( int i=0;i<maxSlaves;i++){

                Object msg = null;

                if (!messages.isEmpty())
                    msg = messages.poll();


                if(msg != null) {

                    ActorRef newActor = RetrieveAndProcessActor.createActor(getContext().system(),
                            httpRetrieveResponseFactory, mediaStorageClient, nodeMasterConfig.getColorMapPath()
                            );
                    addActorToReferenceArray(newActor);

                    context().watch(newActor);

                    newActor.tell(msg, getSelf());
                }

            }

            LOG.info("Messages: {}", messages.size());
            LOG.info("All ProcessorActors: {} ", actors.size());
        }
    }

    private void onRequestTasksReceived() {
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

    private void onDoneDownloadReceived(DoneDownload message) {
        final DoneDownload doneDownload = message;
        final String jobId = doneDownload.getJobId();

        if(!jobsToStop.contains(jobId)) {

            masterReceiver.tell(new DownloadConfirmation(doneDownload.getTaskID(), doneDownload.getIpAddress(),doneDownload.getHttpRetrieveResponse().getState()), getSelf());

            if((DocumentReferenceTaskType.CHECK_LINK).equals(doneDownload.getDocumentReferenceTask().getTaskType()) ||
                    (ProcessingState.ERROR).equals(doneDownload.getProcessingState())) {
                LOG.info("Slave sending DoneDownload message for job {} and task {}", doneDownload.getJobId(), doneDownload.getTaskID());
                masterReceiver.tell(new DoneProcessing(doneDownload, null, null, null, null), getSelf());
                deleteFile(doneDownload.getReferenceId());

            }

        }
        SlaveMetrics.Worker.Master.doneDownloadStateCounters.get(message.getHttpRetrieveResponse().getState()).mark();
        SlaveMetrics.Worker.Master.doneDownloadTotalCounter.mark();
    }

    private void onDoneProcessingReceived(Object message) {
        final DoneProcessing doneProcessing = (DoneProcessing)message;
        final String jobId = doneProcessing.getJobId();

        removeActorFromReferenceArray(getSender());

        if(!jobsToStop.contains(jobId)) {
            masterReceiver.tell(message, getSelf());
            LOG.info("Slave sending DoneProcessing message for job {} and task {}", doneProcessing.getJobId(), doneProcessing.getTaskID());
        }

        SlaveMetrics.Worker.Master.doneProcessingStateCounters.get(doneProcessing.getProcessingState()).mark();
        SlaveMetrics.Worker.Master.doneProcessingTotalCounter.mark();
    }

    private void onCleanReceived() {
        LOG.info("Cleaning up slave");

        context().system().stop(getSelf());
    }

    private void onSendHeartBeatReceived() {
        getSender().tell(new SlaveHeartbeat(), getSelf());
    }

    private void onChangeJobStateReceived(ChangeJobState message) {
        final ChangeJobState changeJobState = message;
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
    }

    private void addActorToReferenceArray(final ActorRef actorRef) {
        this.actors.add(actorRef);
        SlaveMetrics.Worker.Master.activeWorkerSlavesCounter.inc();
    }

    private void removeActorFromReferenceArray(final ActorRef actorRef) {
        this.actors.remove(actorRef);
        SlaveMetrics.Worker.Master.activeWorkerSlavesCounter.dec();
    }

    private void deleteFile(String fileName) {
        final String path = nodeMasterConfig.getPathToSave() + "/" + fileName;
        final File file = new File(path);
        if(file.exists()) {
            file.delete();
        }
    }



}
