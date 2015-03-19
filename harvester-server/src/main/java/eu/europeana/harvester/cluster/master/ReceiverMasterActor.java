package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.DownloadConfirmation;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.StartedTask;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.*;
import org.apache.james.mime4j.dom.datetime.DateTime;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class ReceiverMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;

    /**
     * A map with all system addresses which maps each address with a list of actor refs.
     * This is needed if we want to clean them or if we want to broadcast a message.
     */
    private final Map<Address, HashSet<ActorRef>> actorsPerAddress;

    /**
     * A map with all system addresses which maps each address with a set of tasks.
     * This is needed to restore the tasks if a system crashes.
     */
    private final Map<Address, HashSet<String>> tasksPerAddress;

    /**
     * A map with all sent but not confirmed tasks which maps these tasks with a datetime object.
     * It's needed to restore all the tasks which are not confirmed after a given period of time.
     */
    private final Map<String, DateTime> tasksPerTime;

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
    //private final ESBClient esbClient;

    /**
     * ONLY FOR DEBUG
     *
     * Number of finished tasks.
     * Number of finished tasks with error.
     */
    private Integer success = 0;
    private Integer error = 0;

    public ReceiverMasterActor(final ClusterMasterConfig clusterMasterConfig,
                               final ActorRef accountantActor,
                               final Map<Address, HashSet<ActorRef>> actorsPerAddress,
                               final Map<Address, HashSet<String>> tasksPerAddress,
                               final Map<String, DateTime> tasksPerTime,
                               final ProcessingJobDao processingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao ){
        //                          final ESBClient esbClient) {
        LOG.info("ReceiverMasterActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.actorsPerAddress = actorsPerAddress;
        this.tasksPerAddress = tasksPerAddress;
        this.tasksPerTime = tasksPerTime;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        //this.esbClient = esbClient;
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
            accountantActor.tell(new ModifyState(((DownloadConfirmation) message).getTaskID(), TaskState.PROCESSING), getSelf());
            accountantActor.tell(new RemoveDownloadSpeed(((DownloadConfirmation) message).getTaskID()), getSelf());

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
            final HashSet<ActorRef> actorRefs = new HashSet<>();
            actorRefs.add(actorRef);

            actorsPerAddress.put(address, actorRefs);
        }
    }

    /**
     * Stores a reference to a tasks which can be reached by an actor system address
     * @param address actor systems address
     * @param startedTask started task
     */
    private void addTask(final Address address, final StartedTask startedTask) {
        tasksPerTime.remove(startedTask.getTaskID());

        if(tasksPerAddress.containsKey(address)) {
            final HashSet<String> tasks = tasksPerAddress.get(address);
            tasks.add(startedTask.getTaskID());

            tasksPerAddress.put(address, tasks);
        } else {
            final HashSet<String> tasks = new HashSet<>();
            tasks.add(startedTask.getTaskID());

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
            final HashSet<String> tasks = tasksPerAddress.get(address);
            tasks.remove(processing.getTaskID());

            tasksPerAddress.put(address, tasks);
        }
    }

    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void markDone(DoneProcessing msg) {
        final SourceDocumentReference finishedDocument = sourceDocumentReferenceDao.findByUrl(msg.getUrl());

        final String jobId = msg.getJobId();
        final String docId = finishedDocument.getId();

        try {
            if ((ProcessingState.SUCCESS).equals(msg.getProcessingState())) {

                success += 1;
            } else {
                error += 1;
            }
            accountantActor.tell(new ModifyState(msg.getTaskID(), TaskState.DONE), getSelf());
        } catch (Exception e) {
            LOG.error(e.getMessage());
            return;
        }

        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), finishedDocument.getActive(),
                        msg.getTaskType(), msg.getProcessingState(), finishedDocument.getReferenceOwner(),
                        finishedDocument.getUrlSourceType(), docId,
                        msg.getJobId(), msg.getHttpResponseCode(), msg.getHttpResponseContentType(),
                        msg.getHttpResponseContentSizeInBytes(),
                        msg.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        msg.getRetrievalDurationInMilliSecs(), msg.getCheckingDurationInMilliSecs(),
                        msg.getSourceIp(), msg.getHttpResponseHeaders(), msg.getLog());

        sourceDocumentProcessingStatisticsDao.createOrUpdate(sourceDocumentProcessingStatistics,
                clusterMasterConfig.getWriteConcern());

        saveMetaInfo(docId, msg);

        SourceDocumentReference updatedDocument =
                finishedDocument.withLastStatsId(sourceDocumentProcessingStatistics.getId());
        updatedDocument = updatedDocument.withRedirectionPath(msg.getRedirectionPath());
        sourceDocumentReferenceDao.update(updatedDocument, clusterMasterConfig.getWriteConcern());

        List<TaskState> taskStates = new ArrayList<>();
        final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
        Future<Object> future = Patterns.ask(accountantActor, new GetTaskStatesPerJob(jobId), timeout);
        try {
            taskStates = (List<TaskState>) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("Error at markDone->GetTaskStatesPerJob: {}", e);
        }

        RetrieveUrl retrieveUrl = null;
        future = Patterns.ask(accountantActor, new GetTask(msg.getTaskID()), timeout);
        try {
            retrieveUrl = (RetrieveUrl) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("Error at markDone->GetTask: {}", e);
        }

        if(retrieveUrl != null && !retrieveUrl.getId().equals("")) {
            final String ipAddress = retrieveUrl.getIpAddress();
            checkJobStatus(jobId, taskStates, ipAddress);
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
            LOG.error("Error at markDone->GetConcreteTask: {}", e);
        }

        if(documentReference == null || documentReference.getSourceDocumentReferenceID().equals("")) {
            if(msg.getAudioMetaInfo() != null || msg.getImageMetaInfo() != null ||
                    msg.getVideoMetaInfo() != null || msg.getTextMetaInfo() != null) {
                final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                        new SourceDocumentReferenceMetaInfo(docId, msg.getImageMetaInfo(),
                                msg.getAudioMetaInfo(), msg.getVideoMetaInfo(), msg.getTextMetaInfo());
                final boolean success = sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo,
                        clusterMasterConfig.getWriteConcern());

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

                    if (!success) {
                        sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo,
                                clusterMasterConfig.getWriteConcern());
                    }
                }
            }
        }
    }

    /**
     * Checks if a job is done, and if it's done than generates an event.
     * @param jobID the unique ID of the job
     * @param states all request states the job
     */
    private void checkJobStatus(final String jobID, final List<TaskState> states, final String ipAddress) {
        boolean allDone = true;
        for (final TaskState state : states) {
            if(!(TaskState.DONE).equals(state)) {
                allDone = false;
                break;
            }
        }

        if(allDone) {
            final ProcessingJob processingJob = processingJobDao.read(jobID);
            //only for debug
            LOG.info("Finished with success: {}, with error: {}", success, error);

            final ProcessingJob newProcessingJob = processingJob.withState(JobState.FINISHED);
            processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

            List<String> tasks = new ArrayList<>();
            final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));

            final Future<Object> future = Patterns.ask(accountantActor, new GetTasksFromJob(jobID), timeout);
            try {
                tasks = (List<String>) Await.result(future, timeout.duration());
            } catch (Exception e) {
                LOG.error("Error at markDone->GetTasksFromJob: {}", e);
            }

            if(tasks != null) {
                for (final String taskID : tasks) {
                    accountantActor.tell(new RemoveTask(taskID), getSelf());
                    accountantActor.tell(new RemoveTaskFromIP(taskID, ipAddress), getSelf());
                }
            }
            accountantActor.tell(new RemoveJob(newProcessingJob.getId()), getSelf());


//            final Message message = new Message();
//            message.setStatus(Status.SUCCESS);
//            message.setJobId(newProcessingJob.getId());
//
//            if(esbClient != null) {
//                esbClient.send(message);
//            }
        }
    }
}
