package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.messages.inner.MarkJobAsDone;
import eu.europeana.harvester.cluster.domain.messages.inner.ModifyState;
import eu.europeana.harvester.cluster.master.metrics.MasterMetrics;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.domain.ProcessingJobSubTaskState;
import eu.europeana.harvester.domain.ProcessingJobSubTaskStats;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class ReceiverMasterActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;
    private final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao;

    /**
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;


    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final ProcessingJobDao processingJobDao;

    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final HistoricalProcessingJobDao historicalProcessingJobDao;

    /**
     * SourceDocumentProcessingStatistics DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    /**
     * SourceDocumentReference DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    /**
     * SourceDocumentReferenceMetaInfo DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    private ActorRef receiverJobDumper;

    private ActorRef monitoringActor;

    public ReceiverMasterActor(final ClusterMasterConfig clusterMasterConfig,
                               final ActorRef accountantActor,
                               final ActorRef monitoringActor,
                               final ProcessingJobDao processingJobDao,
                               final HistoricalProcessingJobDao historicalProcessingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao,
                               final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao
                               ){
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMasterActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.monitoringActor = monitoringActor;
        this.processingJobDao = processingJobDao;
        this.historicalProcessingJobDao = historicalProcessingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.lastSourceDocumentProcessingStatisticsDao = lastSourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
    }



    @Override
    public void preStart() throws Exception {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMasterActor prestart");




        receiverJobDumper = getContext().actorOf(Props.create(ReceiverJobDumperActor.class, clusterMasterConfig,
                processingJobDao, historicalProcessingJobDao,sourceDocumentProcessingStatisticsDao, lastSourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao), "jobDumper");

    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMasterActor prestart");

        getContext().system().stop(receiverJobDumper);
    }


        @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof StartedTask) {
            final StartedTask startedTask = (StartedTask) message;
            final Address address = getSender().path().address();

            addAddress(address, getSender());
            addTask(address, startedTask);

            return;
        }
        if(message instanceof DownloadConfirmation) {
            final DownloadConfirmation downloadConfirmation = (DownloadConfirmation) message;
            accountantActor.tell(new ModifyState(downloadConfirmation.getTaskID(),"", "",null, TaskState.PROCESSING), getSelf());
            MasterMetrics.Master.doneDownloadStateCounters.get(downloadConfirmation.getState()).inc();
            MasterMetrics.Master.doneDownloadTotalCounter.inc();

            return;
        }
        if(message instanceof DoneProcessing) {
            final Address address = getSender().path().address();
            final DoneProcessing doneProcessing = (DoneProcessing) message;


            markDone(doneProcessing);

            removeTask(address, doneProcessing);
            MasterMetrics.Master.doneProcessingStateCounters.get(doneProcessing.getProcessingState()).inc();
            MasterMetrics.Master.doneProcessingTotalCounter.inc();

            final ProcessingJobSubTaskStats subTaskStats = doneProcessing.getProcessingStats();
            if (subTaskStats != null) {

                if (subTaskStats.getRetrieveState() != null) {
                    MasterMetrics.Master.doneProcessingRetrieveStateCounters.get(subTaskStats.getRetrieveState()).inc();
                    if (subTaskStats.getRetrieveState() != ProcessingJobSubTaskState.NEVER_EXECUTED) MasterMetrics.Master.doneProcessingRetrieveTotalCounter.inc();
                }

                if (subTaskStats.getColorExtractionState() != null) {
                    MasterMetrics.Master.doneProcessingColorExtractionStateCounters.get(subTaskStats.getColorExtractionState()).inc();
                    if (subTaskStats.getColorExtractionState() != ProcessingJobSubTaskState.NEVER_EXECUTED) MasterMetrics.Master.doneProcessingColorExtractionTotalCounter.inc();
                }


                if (subTaskStats.getMetaExtractionState() != null) {
                    MasterMetrics.Master.doneProcessingMetaExtractionStateCounters.get(subTaskStats.getMetaExtractionState()).inc();
                    if (subTaskStats.getMetaExtractionState() != ProcessingJobSubTaskState.NEVER_EXECUTED) MasterMetrics.Master.doneProcessingMetaExtractionTotalCounter.inc();
                }

                if (subTaskStats.getThumbnailGenerationState() != null) {
                    MasterMetrics.Master.doneProcessingThumbnailGenerationStateCounters.get(subTaskStats.getThumbnailGenerationState()).inc();
                    if (subTaskStats.getThumbnailGenerationState() != ProcessingJobSubTaskState.NEVER_EXECUTED) MasterMetrics.Master.doneProcessingThumbnailGenerationTotalCounter.inc();
                }

                if (subTaskStats.getThumbnailStorageState() != null) {
                    MasterMetrics.Master.doneProcessingThumbnailStorageStateCounters.get(subTaskStats.getThumbnailStorageState()).inc();
                    if (subTaskStats.getThumbnailStorageState() != ProcessingJobSubTaskState.NEVER_EXECUTED) MasterMetrics.Master.doneProcessingThumbnailStorageTotalCounter.inc();
                }

            }

            return;
        }

            if (message instanceof MarkJobAsDone ) {
                receiverJobDumper.tell(message,ActorRef.noSender());

            }
    }

    /**
     * Stores an address and an actorRef from that address.
     * @param address actor systems address
     * @param actorRef reference to an actor from the actor system
     */
    private void addAddress(final Address address, final ActorRef actorRef) {
        monitoringActor.tell(new AddAddressToMonitor(address, actorRef), ActorRef.noSender());
    }

    /**
     * Stores a reference to a tasks which can be reached by an actor system address
     * @param address actor systems address
     * @param startedTask started task
     */
    private void addTask(final Address address, final StartedTask startedTask) {
        monitoringActor.tell(new AddTaskToMonitor(address,startedTask.getTaskID()), ActorRef.noSender());
    }

    /**
     * Removes the reference of a task after it was finished by a slave
     * @param address actor systems address
     * @param processing response object
     */
    private void removeTask(final Address address, final DoneProcessing processing) {
        monitoringActor.tell(new RemoveTaskFromMonitor(address,processing.getTaskID()), ActorRef.noSender());
    }



    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void markDone(DoneProcessing msg) {
            accountantActor.tell(new ModifyState(msg.getTaskID(),msg.getJobId(),msg.getSourceIp(),msg, TaskState.DONE), getSelf());

    }


}
