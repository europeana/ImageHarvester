package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.UntypedActor;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;

public class ReceiverJobDumperActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;


    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final ProcessingJobDao processingJobDao;

    private final HistoricalProcessingJobDao historicalProcessingJobDao;

    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;
    private final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao;
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;


    public ReceiverJobDumperActor(final ClusterMasterConfig clusterMasterConfig,
                                  final ProcessingJobDao processingJobDao,
                                  final HistoricalProcessingJobDao historicalProcessingJobDao,
                                  final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                  final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao,
                                  final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                                  final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao
    ) {
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverJobDumperActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.processingJobDao = processingJobDao;
        this.historicalProcessingJobDao = historicalProcessingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.lastSourceDocumentProcessingStatisticsDao = lastSourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        LOG.debug("receiverjobdumperactor, onreceive");
        if (message instanceof DoneProcessing) {
            LOG.debug("receiverjobdumperactor, message instance of doneprocessing, message url: {}", ((DoneProcessing) message).getUrl());
            DoneProcessing doneProcessing = (DoneProcessing) message;
            markDone(doneProcessing);

        }

        return;
    }


    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     *
     * @param doneProcessing - the message from the slave actor with jobId
     */
    private void markDone(DoneProcessing doneProcessing) {
        // (Step 1) Updating processing jobs

        LOG.debug("receiverjobdumperactor, markdone");

        final ProcessingJob processingJob = processingJobDao.read(doneProcessing.getJobId());
        final ProcessingJob newProcessingJob = processingJob.withState(DoneProcessing.convertProcessingStateToJobState(doneProcessing.getProcessingState()));
        processingJobDao.createOrModify(newProcessingJob, WriteConcern.NORMAL);

        // (Step 2) Updating statistics jobs
        final SourceDocumentReference sourceDocumentReference = saveStatistics(doneProcessing,processingJob);

        // (Step 3) Updating the stats jobs
        saveMetaInfo(doneProcessing);

        LOG.debug("receiverjobdumperactor, doneProcessing.getProcessingState(): {}", doneProcessing.getProcessingState().name());
    }

    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     *
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private SourceDocumentReference saveStatistics(DoneProcessing msg, ProcessingJob processingJob) {
        final ProcessingJobSubTaskStats subTaskStats = msg.getStats();

        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(
                        new Date(),
                        new Date(),
                        true,
                        msg.getTaskType(),
                        msg.getProcessingState(),
                        processingJob.getReferenceOwner(),
                        processingJob.getUrlSourceType(),
                        msg.getReferenceId(),
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
                        subTaskStats
                );

        LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics = new LastSourceDocumentProcessingStatistics(sourceDocumentProcessingStatistics);

        /* We need to keep the previous last stats subtask states as a successful conditional download has them all se to never executed. See #CRF-509 */
        if (lastSourceDocumentProcessingStatistics.getTaskType() == DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD &&
                lastSourceDocumentProcessingStatistics.getState() == ProcessingState.SUCCESS) {
            final LastSourceDocumentProcessingStatistics existingSourceDocumentProcessingStatistics = lastSourceDocumentProcessingStatisticsDao.read(lastSourceDocumentProcessingStatistics.getId());
            if (existingSourceDocumentProcessingStatistics != null ) lastSourceDocumentProcessingStatistics = lastSourceDocumentProcessingStatistics.withProcessingJobSubTaskStats(existingSourceDocumentProcessingStatistics.getProcessingJobSubTaskStats());
        }

        SourceDocumentReference sourceDocumentReference = sourceDocumentReferenceDao.read(msg.getReferenceId());
        if (sourceDocumentReference != null) {
            sourceDocumentReference = sourceDocumentReference.withLastStatsId(sourceDocumentProcessingStatistics.getId()).withRedirectionPath(msg.getRedirectionPath());
        }

        sourceDocumentProcessingStatisticsDao.createOrModify(sourceDocumentProcessingStatistics,
                clusterMasterConfig.getWriteConcern());

        lastSourceDocumentProcessingStatisticsDao.createOrModify(lastSourceDocumentProcessingStatistics, clusterMasterConfig.getWriteConcern());

        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, clusterMasterConfig.getWriteConcern());
        return sourceDocumentReference;

    }

    /**
     * Saves the meta information of a document
     *
\     * @param msg   all the information retrieved while downloading
     */
    private void saveMetaInfo(final DoneProcessing msg) {
        if (new MediaMetaInfoTuple(msg.getImageMetaInfo(), msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo()).isValid()) {
            final SourceDocumentReferenceMetaInfo newSourceDocumentReferenceMetaInfo = new SourceDocumentReferenceMetaInfo(msg.getReferenceId(), msg.getImageMetaInfo(),
                    msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo());
            sourceDocumentReferenceMetaInfoDao.createOrModify(Collections.singleton(newSourceDocumentReferenceMetaInfo),
                    clusterMasterConfig.getWriteConcern());
        }
    }


}
