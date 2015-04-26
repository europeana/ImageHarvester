package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.inner.GetConcreteTask;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class ReceiverMetaInfoDumperActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    final ActorRef accountantActor;

    /**
     * SourceDocumentReferenceMetaInfo DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    final SourceDocumentReferenceDao sourceDocumentReferenceDao;



    public ReceiverMetaInfoDumperActor(final ClusterMasterConfig clusterMasterConfig,
                                       final ActorRef accountantActor,
                                       final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                                       final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao){
        LOG.info("ReceiverMetaInfoDumperActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.accountantActor = accountantActor;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof DoneProcessing) {

            final DoneProcessing doneProcessing = (DoneProcessing) message;

            final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.findByUrl(doneProcessing.getUrl());


            final String docId = finishedDocument.getId();


            saveMetaInfo(docId,doneProcessing);


            return;
        }
    }




    /**
     * Saves the meta information of a document
     * @param docId the unique id of a source document
     * @param msg all the information retrieved while downloading
     */
    private void saveMetaInfo(final String docId, final DoneProcessing msg) {

        ProcessingJobTaskDocumentReference documentReference = null;
        final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));

        final Future<Object> future = Patterns.ask(accountantActor, new GetConcreteTask(msg.getTaskID()), timeout);
        try {
            documentReference = (ProcessingJobTaskDocumentReference) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("Error at saveMetaInfo->GetConcreteTask: {}", e);
        }

        if(documentReference == null || documentReference.getSourceDocumentReferenceID().equals("")) {
            if(msg.getAudioMetaInfo() != null || msg.getImageMetaInfo() != null ||
                    msg.getVideoMetaInfo() != null || msg.getTextMetaInfo() != null) {
                final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                        new SourceDocumentReferenceMetaInfo(docId, msg.getImageMetaInfo(),
                                msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo());
                final boolean success = sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo,
                        clusterMasterConfig.getWriteConcern());

                //LOG.info("Saving MetaInfo for docid {} with success {}", docId, success);

                if(!success) {
                    sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo,
                            clusterMasterConfig.getWriteConcern());
                }
            }
        } else {
            if(msg.getAudioMetaInfo() != null || msg.getImageMetaInfo() != null ||
                    msg.getVideoMetaInfo() != null || msg.getTextMetaInfo() != null) {
                final DocumentReferenceTaskType documentReferenceTaskType = documentReference.getTaskType();
                if (!(DocumentReferenceTaskType.CHECK_LINK).equals(documentReferenceTaskType)) {
                    final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                            new SourceDocumentReferenceMetaInfo(docId, msg.getImageMetaInfo(),
                                    msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo());
                    final boolean success = sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo,
                            clusterMasterConfig.getWriteConcern());

                    //LOG.info("Saving MetaInfo for docid {} with success {}", docId, success);

                    if (!success) {
                        sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo,
                                clusterMasterConfig.getWriteConcern());
                    }
                }
            }
        }
    }

}
