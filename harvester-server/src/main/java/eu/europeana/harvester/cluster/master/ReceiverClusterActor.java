package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.StartedUrl;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.servicebus.client.ESBClient;
import eu.europeana.servicebus.model.Message;
import eu.europeana.servicebus.model.Status;
import org.apache.james.mime4j.dom.datetime.DateTime;

import java.util.*;

public class ReceiverClusterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * A map with all jobs which maps each job with an other map. The inner map contains all the links with their state.
     */
    private final Map<String, Map<String,JobState>> allJobs;

    /**
     * A map with all system addresses which maps each address with a list of actor refs.
     * This is needed if we want to clean them or if we want to broadcast a message.
     */
    private final Map<Address, HashSet<ActorRef>> actorsPerAddress;

    /**
     * A map with all system addresses which maps each address with a set of tasks.
     * This is needed to restore the tasks if a system crashes.
     */
    private final Map<Address, HashSet<Pair<String, String>>> tasksPerAddress;

    /**
     * A map with all sent but not confirmed tasks which maps these tasks with a datetime object.
     * It's needed to restore all the tasks which are not confirmed after a given period of time.
     */
    private final Map<Pair<String, String>, DateTime> tasksPerTime;

    /**
     * Map of urls and their task type(link check, conditional or unconditional download).
     */
    private final Map<String, Map<String, DocumentReferenceTaskType>> taskTypeOfDoc;

    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final ProcessingJobDao processingJobDao;

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

    /**
     * Used to send messages after each finished job.
     */
    private final ESBClient esbClient;

    public ReceiverClusterActor(final ClusterMasterConfig clusterMasterConfig,
                                final Map<String, Map<String, JobState>> allJobs,
                                final Map<Address, HashSet<ActorRef>> actorsPerAddress,
                                final Map<Address, HashSet<Pair<String, String>>> tasksPerAddress,
                                final Map<Pair<String, String>, DateTime> tasksPerTime,
                                final Map<String, Map<String, DocumentReferenceTaskType>> taskTypeOfDoc,
                                final ProcessingJobDao processingJobDao,
                                final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                                final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                                final ESBClient esbClient) {
        this.clusterMasterConfig = clusterMasterConfig;
        this.allJobs = allJobs;
        this.actorsPerAddress = actorsPerAddress;
        this.tasksPerAddress = tasksPerAddress;
        this.tasksPerTime = tasksPerTime;
        this.taskTypeOfDoc = taskTypeOfDoc;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.esbClient = esbClient;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof StartedUrl) {
            final StartedUrl startedUrl = (StartedUrl) message;
            final Address address = getSender().path().address();

            addAddress(address, getSender());
            addTask(address, startedUrl);

            return;
        }
        if(message instanceof DoneProcessing) {
            final Address address = getSender().path().address();
            final DoneProcessing doneProcessing = (DoneProcessing) message;

            markDone(doneProcessing);

            removeTask(address, doneProcessing);

            return;
        }
    }

    /**
     * Stores an address and an actorRef from that address.
     * @param address actor systems address
     * @param actorRef reference to an actor from the actor system
     */
    private void addAddress(final Address address, final ActorRef actorRef) {
        if(actorsPerAddress.containsKey(address)) {
            final HashSet<ActorRef> actorRefs = actorsPerAddress.get(address);
            actorRefs.add(actorRef);

            actorsPerAddress.put(address, actorRefs);
        } else {
            final HashSet<ActorRef> actorRefs = new HashSet<ActorRef>();
            actorRefs.add(actorRef);

            actorsPerAddress.put(address, actorRefs);
        }
    }

    /**
     * Stores a reference to a tasks which can be reached by an actor system address
     * @param address actor systems address
     * @param startedUrl started task
     */
    private void addTask(final Address address, final StartedUrl startedUrl) {
        final Pair<String, String> task = new Pair<String, String>(startedUrl.getJobId(), startedUrl.getSourceDocId());
        tasksPerTime.remove(task);

        if(tasksPerAddress.containsKey(address)) {
            final HashSet<Pair<String, String>> tasks = tasksPerAddress.get(address);
            tasks.add(task);

            tasksPerAddress.put(address, tasks);
        } else {
            final HashSet<Pair<String, String>> tasks = new HashSet<Pair<String, String>>();
            tasks.add(task);

            tasksPerAddress.put(address, tasks);
        }
    }

    /**
     * Removes the reference of a task after it was finished by a slave
     * @param address actor systems address
     * @param processing response object
     */
    private void removeTask(final Address address, final DoneProcessing processing) {
        if(tasksPerAddress.containsKey(address)) {
            final HashSet<Pair<String, String>> tasks = tasksPerAddress.get(address);
            final Pair<String, String> task =
                    new Pair<String, String>(processing.getJobId(), processing.getReferenceId());

            tasks.remove(task);

            tasksPerAddress.put(address, tasks);
        }
    }

    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void markDone(DoneProcessing msg) {
        final ProcessingJob processingJob = processingJobDao.read(msg.getJobId());
        final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.findByUrl(msg.getUrl());

        final String jobId = msg.getJobId();
        final String docId = finishedDocument.getId();

        try {
            if(msg.getProcessingState().equals(ProcessingState.SUCCESS)) {
                allJobs.get(jobId).put(docId, JobState.FINISHED);
            } else {
                allJobs.get(jobId).put(docId, JobState.ERROR);
            }
        } catch(Exception e) {
            LOG.error(e.getMessage());
            return;
        }

        // CreateOrModify statistics
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(docId, jobId);

        if(sourceDocumentProcessingStatistics == null) {
            sourceDocumentProcessingStatistics =
                    new SourceDocumentProcessingStatistics(new Date(), new Date(), msg.getProcessingState(),
                            finishedDocument.getReferenceOwner(), docId, msg.getJobId(),
                            msg.getHttpResponseCode(), msg.getHttpResponseContentType(),
                            msg.getHttpResponseContentSizeInBytes(),
                            msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                            msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                            msg.getSourceIp(), msg.getHttpResponseHeaders(), msg.getLog());

            sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics,
                    clusterMasterConfig.getWriteConcern());
        } else {
            SourceDocumentProcessingStatistics updatedSourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatistics.withUpdate(msg.getProcessingState(), msg.getJobId(),
                            msg.getHttpResponseCode(), msg.getHttpResponseContentSizeInBytes(),
                            msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                            msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                            msg.getHttpResponseHeaders(), msg.getLog());

            sourceDocumentProcessingStatisticsDao.update(updatedSourceDocumentProcessingStatistics,
                    clusterMasterConfig.getWriteConcern());
        }

        saveMetaInfo(docId, msg);

        SourceDocumentReference updatedDocument =
                finishedDocument.withLastStatsId(sourceDocumentProcessingStatistics.getId());
        updatedDocument = updatedDocument.withRedirectionPath(msg.getRedirectionPath());
        sourceDocumentReferenceDao.update(updatedDocument, clusterMasterConfig.getWriteConcern());

        final Map<String, JobState> links = allJobs.get(jobId);
        checkJobStatus(processingJob, links);
    }

    /**
     * Saves the meta information of a document
     * @param docId the unique id of a source document
     * @param msg all the information retrieved while downloading
     */
    private void saveMetaInfo(String docId, DoneProcessing msg) {
        final DocumentReferenceTaskType documentReferenceTaskType = taskTypeOfDoc.get(docId).get(msg.getJobId());
        if(!documentReferenceTaskType.equals(DocumentReferenceTaskType.CHECK_LINK)) {
            final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                    new SourceDocumentReferenceMetaInfo(docId, msg.getImageMetaInfo(),
                            msg.getAudioMetaInfo(), msg.getVideoMetaInfo());
            final boolean success = sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo,
                    clusterMasterConfig.getWriteConcern());

            if(!success) {
                sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo,
                        clusterMasterConfig.getWriteConcern());
            }
        }
    }

    /**
     * Checks if a job is done, and if it's done than generates an event.
     * @param processingJob one job
     * @param links all links from the job
     */
    private void checkJobStatus(ProcessingJob processingJob, Map<String, JobState> links) {
        boolean allDone = true;
        for (final Map.Entry link : links.entrySet()) {
            if(link.getValue() != JobState.FINISHED && link.getValue() != JobState.ERROR) {
                allDone = false;
                break;
            }
        }

        if(allDone) {
            final ProcessingJob newProcessingJob = processingJob.withState(JobState.FINISHED);

            processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

            final Message message = new Message();
            message.setStatus(Status.SUCCESS);
            message.setJobId(newProcessingJob.getId());

            if(esbClient != null) {
                esbClient.send(message);
            }
        }
    }
}
