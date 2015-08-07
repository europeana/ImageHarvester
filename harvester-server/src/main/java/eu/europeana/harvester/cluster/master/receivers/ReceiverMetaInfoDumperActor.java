package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class ReceiverMetaInfoDumperActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    final ActorRef accountantActor;

    /**
     * SourceDocumentReferenceMetaInfo DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    final SourceDocumentReferenceDao SourceDocumentReferenceDao;


    public ReceiverMetaInfoDumperActor(final ClusterMasterConfig clusterMasterConfig,
                                       final ActorRef accountantActor,
                                       final SourceDocumentReferenceDao SourceDocumentReferenceDao,
                                       final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao) {

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMetaInfoDumperActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.SourceDocumentReferenceDao = SourceDocumentReferenceDao;
        this.accountantActor = accountantActor;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if (message instanceof DoneProcessing) {

            final DoneProcessing doneProcessing = (DoneProcessing) message;

            final SourceDocumentReference finishedDocument = SourceDocumentReferenceDao.read(doneProcessing.getReferenceId());


            final String docId = finishedDocument.getId();


            saveMetaInfo(docId, doneProcessing);


            return;
        }
    }


    /**
     * Saves the meta information of a document
     *
     * @param docId the unique id of a source document
     * @param msg   all the information retrieved while downloading
     */
    private void saveMetaInfo(final String docId, final DoneProcessing msg) {
        if (new MediaMetaInfoTuple(msg.getImageMetaInfo(), msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo()).isValid()) {
            final SourceDocumentReferenceMetaInfo newSourceDocumentReferenceMetaInfo = new SourceDocumentReferenceMetaInfo(docId, msg.getImageMetaInfo(),
                    msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo());
            sourceDocumentReferenceMetaInfoDao.createOrModify(Collections.singleton(newSourceDocumentReferenceMetaInfo),
                    clusterMasterConfig.getWriteConcern());
        }
    }

}
