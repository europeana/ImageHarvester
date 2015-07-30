package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.db.interfaces.LastSourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ReceiverStatisticsDumperActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;


    /**
     * SourceDocumentProcessingStatistics DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    /**
     * SourceDocumentReference DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    private final ProcessingJobDao processingJobDao;
    private final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao;


    public ReceiverStatisticsDumperActor(final ClusterMasterConfig clusterMasterConfig,
                                         final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                         final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao,
                                         final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                                         final ProcessingJobDao processingJobDao){
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverStatisticsDumperActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.lastSourceDocumentProcessingStatisticsDao = lastSourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.processingJobDao = processingJobDao;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof DoneProcessing) {
            final DoneProcessing doneProcessing = (DoneProcessing) message;
            saveStatistics(doneProcessing);
            return;
        }
    }


    private ProcessingJobSubTaskStats computeStats (final DoneProcessing msg) {
       if (!RetrievingState.ERROR.equals(msg.getRetrievingState())) {
           return new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.ERROR);
       }
       else if (!RetrievingState.COMPLETED.equals(msg.getRetrievingState())) {
           return new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.FAILED);
       }
       else if (DocumentReferenceTaskType.CHECK_LINK.equals(msg.getTaskType())) {
            return ProcessingJobSubTaskStats.withRetrievelSuccess();
       }

       return msg.getProcessingStats();
    }

    private ProcessingStatus computeStatus (final ProcessingJobSubTaskStats stats) {
        if (ProcessingJobSubTaskState.SUCCESS != stats.getColorExtractionState() ||
            ProcessingJobSubTaskState.SUCCESS != stats.getMetaExtractionState() ||
            ProcessingJobSubTaskState.SUCCESS != stats.getRetrieveState() ||
            ProcessingJobSubTaskState.SUCCESS != stats.getThumbnailGenerationState() ||
            ProcessingJobSubTaskState.SUCCESS != stats.getThumbnailStorageState()) {
            return ProcessingStatus.Failure;
        }
        return ProcessingStatus.Success;
    }


    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void saveStatistics(DoneProcessing msg) {
        final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.read(msg.getReferenceId());
        final ProcessingJob processingJob = processingJobDao.read(msg.getJobId());

        final String docId = finishedDocument.getId();
        //LOG.info("save statistics for document with ID: {}",docId);

        final ProcessingJobSubTaskStats subTaskStats = computeStats(msg);

        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(
                        new Date(),
                        new Date(),
                        finishedDocument.getActive(),
                        msg.getTaskType(),
                        msg.getProcessingState(),
                        processingJob.getReferenceOwner(),
                        processingJob.getUrlSourceType(),
                        docId,
                        msg.getJobId(),
                        msg.getHttpResponseCode(),
                        msg.getHttpResponseContentType(),
                        msg.getHttpResponseContentSizeInBytes(),
                        msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        msg.getRetrievalDurationInMilliSecs(),
                        msg.getCheckingDurationInMilliSecs(),
                        msg.getSourceIp(),
                        msg.getHttpResponseHeaders(),
                        msg.getLog(),
                        computeStatus(subTaskStats),
                        subTaskStats
                );

        sourceDocumentProcessingStatisticsDao.createOrModify(sourceDocumentProcessingStatistics,
                clusterMasterConfig.getWriteConcern());


        LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics =
                lastSourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getSourceDocumentReferenceId(),
                                                               sourceDocumentProcessingStatistics.getTaskType(),
                                                               sourceDocumentProcessingStatistics.getUrlSourceType()
                                                              );

        if (null == lastSourceDocumentProcessingStatistics) {
           lastSourceDocumentProcessingStatistics =
                   new LastSourceDocumentProcessingStatistics(new Date(), new Date(), finishedDocument.getActive(), msg.getTaskType(),
                                                       msg.getProcessingState(), processingJob.getReferenceOwner(),
                                                       processingJob.getUrlSourceType(), docId, msg.getJobId(), msg.getHttpResponseCode(),
                                                       msg.getHttpResponseContentType(), msg.getHttpResponseContentSizeInBytes(),
                                                       msg.getSocketConnectToDownloadStartDurationInMilliSecs(), msg.getRetrievalDurationInMilliSecs(),
                                                       msg.getCheckingDurationInMilliSecs(), msg.getSourceIp(), msg.getHttpResponseHeaders(),
                                                       msg.getLog(),
                                                       computeStatus(subTaskStats),
                                                       subTaskStats
                                                      );
        }
        else {
            lastSourceDocumentProcessingStatistics
                    .withUpdate(msg.getProcessingState(), msg.getJobId(), msg.getHttpResponseCode(),
                                msg.getHttpResponseContentSizeInBytes(),
                                msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                                msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                                msg.getHttpResponseHeaders(), msg.getLog(),
                                computeStatus(subTaskStats),
                                subTaskStats
                               );

        }

        lastSourceDocumentProcessingStatisticsDao.createOrModify(lastSourceDocumentProcessingStatistics, clusterMasterConfig.getWriteConcern());

        SourceDocumentReference updatedDocument =
                finishedDocument.withLastStatsId(sourceDocumentProcessingStatistics.getId());
        updatedDocument = updatedDocument.withRedirectionPath(msg.getRedirectionPath());
        sourceDocumentReferenceDao.update(updatedDocument, clusterMasterConfig.getWriteConcern());
    }


}
