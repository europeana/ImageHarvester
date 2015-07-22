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
import eu.europeana.harvester.domain.ProcessingState;
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
    private ActorRef receiverStatisticsDumper;
    private ActorRef receiverMetaInfoDumper;

    private ActorRef monitoringActor;

    /**
     * ONLY FOR DEBUG
     *
     * Number of finished tasks.
     * Number of finished tasks with error.
     */
    private Integer success = 0;
    private Integer error = 0;
    int counter = 0;

    public ReceiverMasterActor(final ClusterMasterConfig clusterMasterConfig,
                               final ActorRef accountantActor,
                               final ActorRef monitoringActor,
                               final ProcessingJobDao processingJobDao,
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
                accountantActor,  processingJobDao), "jobDumper");

        receiverStatisticsDumper = getContext().actorOf(Props.create(ReceiverStatisticsDumperActor.class, clusterMasterConfig,
                sourceDocumentProcessingStatisticsDao, lastSourceDocumentProcessingStatisticsDao,
                sourceDocumentReferenceDao, processingJobDao), "statisticsDumper");

        receiverMetaInfoDumper = getContext().actorOf(Props.create(ReceiverMetaInfoDumperActor.class, clusterMasterConfig,
                accountantActor, sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao), "metaInfoDumper");

    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMasterActor prestart");

        getContext().system().stop(receiverJobDumper);
        getContext().system().stop(receiverMetaInfoDumper);
        getContext().system().stop(receiverStatisticsDumper);
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
            accountantActor.tell(new ModifyState(downloadConfirmation.getTaskID(),"", "", TaskState.PROCESSING), getSelf());
            MasterMetrics.Master.doneDownloadStateCounters.get(downloadConfirmation.getState()).mark();
            MasterMetrics.Master.doneDownloadTotalCounter.mark();

            return;
        }
        if(message instanceof DoneProcessing) {
            final Address address = getSender().path().address();
            final DoneProcessing doneProcessing = (DoneProcessing) message;


            markDone(doneProcessing);

            removeTask(address, doneProcessing);
            MasterMetrics.Master.doneProcessingStateCounters.get(doneProcessing.getProcessingState()).mark();
            MasterMetrics.Master.doneProcessingTotalCounter.mark();
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
//        if(actorsPerAddress.containsKey(address)) {
//            final HashSet<ActorRef> actorRefs = actorsPerAddress.get(address);
//            actorRefs.add(actorRef);
//
//            actorsPerAddress.put(address, actorRefs);
//        } else {
//            final HashSet<ActorRef> actorRefs = new HashSet<>();
//            actorRefs.add(actorRef);
//
//            actorsPerAddress.put(address, actorRefs);
//        }
        monitoringActor.tell(new AddAddressToMonitor(address, actorRef), ActorRef.noSender());
    }

    /**
     * Stores a reference to a tasks which can be reached by an actor system address
     * @param address actor systems address
     * @param startedTask started task
     */
    private void addTask(final Address address, final StartedTask startedTask) {
//        tasksPerTime.remove(startedTask.getTaskID());
//
//        if(tasksPerAddress.containsKey(address)) {
//            final HashSet<String> tasks = tasksPerAddress.get(address);
//            tasks.add(startedTask.getTaskID());
//
//            tasksPerAddress.put(address, tasks);
//        } else {
//            final HashSet<String> tasks = new HashSet<>();
//            tasks.add(startedTask.getTaskID());
//
//            tasksPerAddress.put(address, tasks);
//        }
        monitoringActor.tell(new AddTaskToMonitor(address,startedTask.getTaskID()), ActorRef.noSender());
    }

    /**
     * Removes the reference of a task after it was finished by a slave
     * @param address actor systems address
     * @param processing response object
     */
    private void removeTask(final Address address, final DoneProcessing processing) {
//        if(tasksPerAddress.containsKey(address)) {
//            final HashSet<String> tasks = tasksPerAddress.get(address);
//            tasks.remove(processing.getTaskID());
//
//            tasksPerAddress.put(address, tasks);
//        }
        monitoringActor.tell(new RemoveTaskFromMonitor(address,processing.getTaskID()), ActorRef.noSender());
    }



    /**
     * Marks task as done and save it's statistics in the DB.
     * If one job has finished all his tasks then the job also will be marked as done(FINISHED).
     * @param msg - the message from the slave actor with url, jobId and other statistics
     */
    private void markDone(DoneProcessing msg) {

        try {
            // save the data in Mongo
            //receiverJobDumper.tell(msg,ActorRef.noSender());
            receiverMetaInfoDumper.tell(msg, ActorRef.noSender());
            receiverStatisticsDumper.tell(msg, ActorRef.noSender());
            //update accountant
            accountantActor.tell(new ModifyState(msg.getTaskID(),msg.getJobId(),msg.getSourceIp(), TaskState.DONE), getSelf());


//            Boolean haveTasks = true;
//
//            final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
//
//            Future<Object> future = Patterns.ask(accountantActor, new ModifyState(msg.getTaskID(),msg.getJobId(),msg.getSourceIp(), TaskState.DONE), timeout);
//            try {
//                haveTasks = (Boolean) Await.result(future, timeout.duration());
//            } catch (Exception e) {
//                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
//                        "Error at markDone->ModifyState.", e);
//                // TODO : Investigate if it make sense to hide the exception here.
//            }
//
//            // if at this moment haveTasks is false, it means that we exhausted all tasks for that specific job
//            // so we can mark it as finished in the DB
//            if(!haveTasks) {
//                final ProcessingJob processingJob = processingJobDao.read(msg.getJobId());
//                final ProcessingJob newProcessingJob = processingJob.withState(JobState.FINISHED);
//                processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());
//            }

            if ((ProcessingState.SUCCESS).equals(msg.getProcessingState())) {

                success++;
            } else {
                error++;
            }

            if ( counter+100 < success + error) {
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                        "Finished 100+ tasks with success: {}, with error: {}", success, error);

                counter = success+error;
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
            return;
        }



    }


}
