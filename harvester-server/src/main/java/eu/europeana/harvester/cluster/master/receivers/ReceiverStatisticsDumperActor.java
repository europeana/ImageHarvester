package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import eu.europeana.harvester.domain.SourceDocumentReference;
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




    public ReceiverStatisticsDumperActor(final ClusterMasterConfig clusterMasterConfig,
                                         final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                         final SourceDocumentReferenceDao sourceDocumentReferenceDao){
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverStatisticsDumperActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof DoneProcessing) {
            final DoneProcessing doneProcessing = (DoneProcessing) message;
            saveStatistics(doneProcessing);
            return;
        }
    }



    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void saveStatistics(DoneProcessing msg) {
        final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.findByUrl(msg.getUrl());


        final String docId = finishedDocument.getId();
        //LOG.info("save statistics for document with ID: {}",docId);


        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), finishedDocument.getActive(),
                        msg.getTaskType(), msg.getProcessingState(), finishedDocument.getReferenceOwner(),
                        finishedDocument.getUrlSourceType(), docId,
                        msg.getJobId(), msg.getHttpResponseCode(), msg.getHttpResponseContentType(),
                        msg.getHttpResponseContentSizeInBytes(),
                        msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                        msg.getSourceIp(), msg.getHttpResponseHeaders(), msg.getLog());

        sourceDocumentProcessingStatisticsDao.createOrModify(sourceDocumentProcessingStatistics,
                clusterMasterConfig.getWriteConcern());


        SourceDocumentReference updatedDocument =
                finishedDocument.withLastStatsId(sourceDocumentProcessingStatistics.getId());
        updatedDocument = updatedDocument.withRedirectionPath(msg.getRedirectionPath());
        sourceDocumentReferenceDao.update(updatedDocument, clusterMasterConfig.getWriteConcern());

    }


}
