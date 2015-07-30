package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
import com.codahale.metrics.Gauge;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

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
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                "Slave master constructed.");

        this.masterSender = masterSender;
        this.nodeSupervisor = nodeSupervisor;
        this.nodeMasterConfig = nodeMasterConfig;

        this.jobsToStop = new HashSet<>();

        this.sentRequest = false;
        this.mediaStorageClient = mediaStorageClient;
        this.maxSlaves = nodeMasterConfig.getNrOfDownloaderSlaves();

        // Register the global gauges
        SlaveMetrics.Worker.Master.activeWorkerSlavesCounter.registerHandler(new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return actors.size();
            }
        });

    }

    @Override
    public void preStart() throws Exception {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER), "Slave master preStart.");

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
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER), "Slave master preStart.");
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                "Slave master postStart.");

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
        this.actors.remove(which);

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                "Slave master received worker termination message. Worker Actor {} terminated. Master stats : Messages {}. All ProcessorActors {}. ",t.getActor(),messages.size(),actors.size());

        if (actors.size()<maxSlaves){
            Object msg = null;
            if (!messages.isEmpty())
                msg = messages.poll();

            if(msg != null) {


                ActorRef newActor = RetrieveAndProcessActor.createActor(getContext().system(),
                        httpRetrieveResponseFactory, mediaStorageClient, nodeMasterConfig.getColorMapPath()
                        );
                this.actors.add(newActor);
                context().watch(newActor);
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                        "Slave master starting new Worker Actor {} ",newActor);
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
                    RetrieveUrlWithProcessingConfig tst = (RetrieveUrlWithProcessingConfig) msg;
                    LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                            "Slave master starting new Worker Actor for url {} ",tst.getRetrieveUrl().getUrl());

                    ActorRef newActor = RetrieveAndProcessActor.createActor(getContext().system(),
                            httpRetrieveResponseFactory, mediaStorageClient, nodeMasterConfig.getColorMapPath()
                            );
                    this.actors.add(newActor);

                    context().watch(newActor);

                    newActor.tell(msg, getSelf());
                }

            }
        }
    }

    private void onRequestTasksReceived() {
        //allways request tasks if the message is coming from the supervisor
        if ( getSender().equals(nodeSupervisor)) {
            if ( masterSender!= null && messages.size() < nodeMasterConfig.getTaskNrLimit() ) {
                masterSender.tell(new RequestTasks(), nodeSupervisor);
                sentRequest = true;
                lastRequest = System.currentTimeMillis();
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                        "Slave master requesting tasks from node supervisor ");
            }
        }
        else if(!sentRequest && masterSender != null && messages.size() < nodeMasterConfig.getTaskNrLimit()) {
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                    "Slave master sent request for tasks ");

            masterSender.tell(new RequestTasks(), nodeSupervisor);
            sentRequest = true;
            lastRequest = System.currentTimeMillis();
        }
        else  {
                final Long currentTime = System.currentTimeMillis();
                final int diff = Math.round ( ((currentTime - lastRequest)/1000) );
                if(diff > 5 && messages.size() < nodeMasterConfig.getTaskNrLimit() ) {
                    sentRequest = true;
                    //self().tell(new RequestTasks(), ActorRef.noSender());
                    masterSender.tell(new RequestTasks(), nodeSupervisor);
                    lastRequest = System.currentTimeMillis();
                    LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                            "Slave master requesting tasks time difference ");
                } else {
                    LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                            "No request: " + messages.size() + " " + sentRequest + " task limits: "+
                                    nodeMasterConfig.getTaskNrLimit()+" time diff : " +diff);
                }

        }
    }

    private void onDoneDownloadReceived(DoneDownload message) {
        final DoneDownload doneDownload = message;
        final String jobId = doneDownload.getJobId();

        if(!jobsToStop.contains(jobId)) {

            masterReceiver.tell(new DownloadConfirmation(doneDownload.getTaskID(),
                                                         doneDownload.getIpAddress(),
                                                         doneDownload.getHttpRetrieveResponse().getState()),
                                                         getSelf()
                               );

            if(DocumentReferenceTaskType.CHECK_LINK.equals(doneDownload.getDocumentReferenceTask().getTaskType()) ||
               !(RetrievingState.COMPLETED).equals(doneDownload.getRetrieveState())) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                          "Slave sending DoneProcessing message for job {} and task {}",
                          doneDownload.getJobId(), doneDownload.getTaskID());

                masterReceiver.tell(new DoneProcessing(doneDownload), getSelf());
                deleteFile(doneDownload.getReferenceId());

            }
        }
        SlaveMetrics.Worker.Master.doneDownloadStateCounters.get(message.getHttpRetrieveResponse().getState()).mark();
        SlaveMetrics.Worker.Master.doneDownloadTotalCounter.mark();
    }

    private void onDoneProcessingReceived(Object message) {
        final DoneProcessing doneProcessing = (DoneProcessing)message;
        final String jobId = doneProcessing.getJobId();
        this.actors.remove(getSender());

        if(!jobsToStop.contains(jobId)) {
            masterReceiver.tell(message, getSelf());

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                    "Slave sending DoneProcessing message for job {} and task {}", doneProcessing.getJobId(), doneProcessing.getTaskID());
        }

        SlaveMetrics.Worker.Master.doneProcessingStateCounters.get(doneProcessing.getProcessingState()).mark();
        SlaveMetrics.Worker.Master.doneProcessingTotalCounter.mark();
    }

    private void onCleanReceived() {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                "Cleaning up slave");

        context().system().stop(getSelf());
    }

    private void onSendHeartBeatReceived() {
        getSender().tell(new SlaveHeartbeat(), getSelf());
    }

    private void onChangeJobStateReceived(ChangeJobState message) {
        final ChangeJobState changeJobState = message;
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
                "Changing job state to: {}", changeJobState.getNewState());

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

    private void deleteFile(String fileName) {
        final String path = nodeMasterConfig.getPathToSave() + "/" + fileName;
        final File file = new File(path);
        if(file.exists()) {
            file.delete();
        }
    }



}
