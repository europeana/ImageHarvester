package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
import com.codahale.metrics.Gauge;
import eu.europeana.harvester.cluster.domain.NodeMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotRequest;
import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotResponse;
import eu.europeana.harvester.cluster.master.limiter.domain.ReturnConnectionSlotRequest;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * This acts as a load balancer for the "retrieve and process" actor.
 */
public class NodeMasterActor extends UntypedActor {

        public static ActorRef createActor(final ActorContext context, final ActorRef masterSender,
                                           final ActorRef nodeSupervisor,
                                           final NodeMasterConfig nodeMasterConfig,
                                           final MediaStorageClient mediaStorageClient){

        return context.system().actorOf(Props.create(NodeMasterActor.class,
                        masterSender,nodeSupervisor, nodeMasterConfig, mediaStorageClient),
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
     * Reference to the cluster master actor.
     * We need this to send request for new tasks.
     */
    private ActorRef masterSender;

    /**
     * List of unprocessed jobsReadyToBeProcessed.
     */
    private final Queue<Object> jobsReadyToBeProcessed = new LinkedList<>();

    /**
     * List of jobs which was stopped by the clients.
     */
    private final Set<String> jobsToStop;

    private Boolean sentRequest;

    final private List<ActorRef> actors = new ArrayList<>() ;

    final private HashMap<String, Pair<RetrieveUrlWithProcessingConfig,ReserveConnectionSlotResponse>> taskIDToRetrieveURL = new HashMap<>();


    Long lastRequest;
    final int maxSlaves;

    private MediaStorageClient mediaStorageClient;

    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
    final ExecutorService service = Executors.newCachedThreadPool();

    public NodeMasterActor(final ActorRef masterSender,final  ActorRef nodeSupervisor,
                           final NodeMasterConfig nodeMasterConfig,
                           final MediaStorageClient mediaStorageClient
                           ) {

        this.masterSender = masterSender;
        this.nodeSupervisor = nodeSupervisor;
        this.nodeMasterConfig = nodeMasterConfig;

        this.jobsToStop = new HashSet<>();

        this.sentRequest = false;
        this.mediaStorageClient = mediaStorageClient;
        this.maxSlaves = nodeMasterConfig.getNrOfDownloaderSlaves();

        LOG.debug("SLAVE - Node master actor constructor");

        // Register the global gauges
        SlaveMetrics.Worker.Master.activeWorkerSlavesCounter.registerHandler(new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return actors.size();
            }
        });

        SlaveMetrics.Worker.Master.jobsReadyToBeProcessedCounter.registerHandler(new Gauge<Integer>() {
            @Override
            public Integer getValue() {
                return jobsReadyToBeProcessed.size();
            }
        });

    }

    @Override
    public void preStart() throws Exception {

        lastRequest = 0l;
        sentRequest = false;

        LOG.debug("SLAVE - Node master actor, post restart");

        final int maxNrOfRetries = nodeMasterConfig.getNrOfRetries();
        final SupervisorStrategy strategy =
                new OneForOneStrategy(maxNrOfRetries, scala.concurrent.duration.Duration.create(1, TimeUnit.MINUTES),
                        Collections.<Class<? extends Throwable>>singletonList(Exception.class));
    }


    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {

        LOG.debug("SLAVE - Node master actor, pre restart");

        super.preRestart(reason, message);
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {

        LOG.debug("SLAVE - Node master actor, post restart");

        super.postRestart(reason);

        sentRequest = false;
        lastRequest = 0l;

        self().tell(new RequestTasks(), ActorRef.noSender());
    }

    @Override
    public void onReceive(Object message) throws Exception {

        LOG.debug("SLAVE - Node master actor, message is " + message.toString());

        if(message instanceof RetrieveUrlWithProcessingConfig) {
            onRetrieveUrlWithProcessingConfigReceived((RetrieveUrlWithProcessingConfig)message);
            return;
        }

        if(message instanceof ReserveConnectionSlotResponse ) {
            onReserveConnectionSlotResponseReceived((ReserveConnectionSlotResponse) message);
            return;
        }

        if(message instanceof RequestTasks ) {
            onRequestTasksReceived();
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

        LOG.debug("SLAVE - Node master actor - onTerminatedReceived");

        if (actors.size()<maxSlaves){
            Object msg = null;
            if (!jobsReadyToBeProcessed.isEmpty())
                msg = jobsReadyToBeProcessed.poll();

            if(msg != null) {


                ActorRef newActor = RetrieveAndProcessActor.createActor(getContext().system(),
                        httpRetrieveResponseFactory, mediaStorageClient, nodeMasterConfig.getColorMapPath()
                        );
                this.actors.add(newActor);
                context().watch(newActor);

                newActor.tell(msg, getSelf());
            }
            if ( taskIDToRetrieveURL.size() < nodeMasterConfig.getTaskNrLimit()) {
                self().tell(new RequestTasks(), ActorRef.noSender());
            }

            LOG.debug("SLAVE - Node master actor - onTerminatedReceived, actors < max slaves, task id to retrieve is:  " + taskIDToRetrieveURL);

        }

    }

    private void onRetrieveUrlWithProcessingConfigReceived ( RetrieveUrlWithProcessingConfig retrieveUrl ) {

        LOG.debug("SLAVE - Node master actor - onRetrieveUrlWithProcessingConfigReceived");

        taskIDToRetrieveURL.put(retrieveUrl.getRetrieveUrl().getId(), new Pair(retrieveUrl,null));

        LOG.debug("SLAVE - Node master actor - onRetrieveUrlWithProcessingConfigReceived - taskIDToRetrieveURL has size " +
        taskIDToRetrieveURL.size());

        SlaveMetrics.Worker.Master.jobsWaitingForSlotGrantCounter.inc();
        masterSender.tell(new ReserveConnectionSlotRequest(retrieveUrl.getRetrieveUrl().getIpAddress(),
                retrieveUrl.getRetrieveUrl().getId()),getSelf());
    }

    private void onReserveConnectionSlotResponseReceived ( ReserveConnectionSlotResponse reserveConnectionSlotResponse) {

        LOG.debug("SLAVE - Node master actor - onReserveConnectionSlotResponseReceived");

        if (!taskIDToRetrieveURL.containsKey(reserveConnectionSlotResponse.getTaskID()))
            return;

        RetrieveUrlWithProcessingConfig retrieveUrl = taskIDToRetrieveURL.get(reserveConnectionSlotResponse.getTaskID()).getKey();

        if ( !reserveConnectionSlotResponse.getGranted()) {
            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(30,
                    TimeUnit.SECONDS), masterSender,
                    new ReserveConnectionSlotRequest(reserveConnectionSlotResponse.getIp(),reserveConnectionSlotResponse.getTaskID()),
                    getContext().system().dispatcher(), getSelf());
            return;
        }

        taskIDToRetrieveURL.put(retrieveUrl.getRetrieveUrl().getId(), new Pair(retrieveUrl,reserveConnectionSlotResponse));

        LOG.debug("SLAVE - Node master actor - onReserveConnectionSlotResponseReceived - taskIDToRetrieveURL has size " +
                taskIDToRetrieveURL.size());


        SlaveMetrics.Worker.Master.jobsWaitingForSlotGrantCounter.dec();
        executeRetrieveURL(retrieveUrl);
    }

    private void executeRetrieveURL(Object message) {
        jobsReadyToBeProcessed.add(message);

        if ( actors.size()<maxSlaves & jobsReadyToBeProcessed.size()>maxSlaves ) {

            for ( int i=0;i<maxSlaves;i++){
                Object msg = null;

                if (!jobsReadyToBeProcessed.isEmpty())
                    msg = jobsReadyToBeProcessed.poll();


                if(msg != null) {
                    RetrieveUrlWithProcessingConfig tst = (RetrieveUrlWithProcessingConfig) msg;
                    LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Slave.MASTER),
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

        LOG.debug("SLAVE - Node master actor - onRequestTasksReceived");

        if ( getSender().equals(nodeSupervisor)) {
            if ( masterSender!= null && jobsReadyToBeProcessed.size() < nodeMasterConfig.getTaskNrLimit() ) {
                masterSender.tell(new RequestTasks(), nodeSupervisor);
                sentRequest = true;
                lastRequest = System.currentTimeMillis();
            }
        }
        else if(!sentRequest && masterSender != null && jobsReadyToBeProcessed.size() < nodeMasterConfig.getTaskNrLimit()) {

            masterSender.tell(new RequestTasks(), nodeSupervisor);
            sentRequest = true;
            lastRequest = System.currentTimeMillis();
        }
        else  {
                final Long currentTime = System.currentTimeMillis();
                final int diff = Math.round ( ((currentTime - lastRequest)/1000) );
                if(diff > 5 && jobsReadyToBeProcessed.size() < nodeMasterConfig.getTaskNrLimit() ) {
                    sentRequest = true;
                    //self().tell(new RequestTasks(), ActorRef.noSender());
                    masterSender.tell(new RequestTasks(), nodeSupervisor);
                    lastRequest = System.currentTimeMillis();

                }

        }
    }

    private void onDoneProcessingReceived(Object message) {
        final DoneProcessing doneProcessing = (DoneProcessing)message;

        LOG.debug("SLAVE - Node master actor - ondoneprocessingreceived, message: " + doneProcessing.getProcessingState().name());

        this.actors.remove(getSender());

        if(taskIDToRetrieveURL.containsKey(doneProcessing.getTaskID())) {

            Pair < RetrieveUrlWithProcessingConfig, ReserveConnectionSlotResponse> pair = taskIDToRetrieveURL.remove(doneProcessing.getTaskID());
            masterSender.tell(new ReturnConnectionSlotRequest(pair.getValue().getSlotId(), pair.getValue().getIp()), ActorRef.noSender());
        }

        masterSender.tell(message, getSelf());


        SlaveMetrics.Worker.Master.doneProcessingStateCounters.get(doneProcessing.getProcessingState()).inc();
        SlaveMetrics.Worker.Master.doneProcessingTotalCounter.inc();
    }

    private void onCleanReceived() {

        LOG.debug("SLAVE - Node master actor - oncleanReceived");

        context().system().stop(getSelf());
    }

    private void onSendHeartBeatReceived() {

        LOG.debug("SLAVE - Node master actor - onHeartBeatReceived");

        getSender().tell(new SlaveHeartbeat(), getSelf());
    }

    private void onChangeJobStateReceived(ChangeJobState message) {
        final ChangeJobState changeJobState = message;

        LOG.debug("SLAVE - Node master actor - onchangejobstatereceived, message new state: " + message.getNewState().name());


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





}
