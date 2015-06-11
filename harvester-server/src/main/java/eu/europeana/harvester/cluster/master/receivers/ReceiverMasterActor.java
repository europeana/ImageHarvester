package eu.europeana.harvester.cluster.master.receivers;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.DownloadConfirmation;
import eu.europeana.harvester.cluster.domain.messages.StartedTask;
import eu.europeana.harvester.cluster.domain.messages.inner.ModifyState;
import eu.europeana.harvester.cluster.master.MasterMetrics;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.logging.LoggingComponent;
import org.apache.james.mime4j.dom.datetime.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.util.HashSet;
import java.util.Map;

public class ReceiverMasterActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

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

    private ActorRef receiverJobDumper;
    private ActorRef receiverStatisticsDumper;
    private ActorRef receiverMetaInfoDumper;

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
                               final Map<Address, HashSet<ActorRef>> actorsPerAddress,
                               final Map<Address, HashSet<String>> tasksPerAddress,
                               final Map<String, DateTime> tasksPerTime,
                               final ProcessingJobDao processingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao
                               ){
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMasterActor constructor");

        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.actorsPerAddress = actorsPerAddress;
        this.tasksPerAddress = tasksPerAddress;
        this.tasksPerTime = tasksPerTime;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
    }



    @Override
    public void preStart() throws Exception {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_RECEIVER),
                "ReceiverMasterActor prestart");

        receiverJobDumper = getContext().system().actorOf(Props.create(ReceiverJobDumperActor.class, clusterMasterConfig,
                accountantActor,  processingJobDao), "jobDumper");

        receiverStatisticsDumper = getContext().system().actorOf(Props.create(ReceiverStatisticsDumperActor.class, clusterMasterConfig,
                sourceDocumentProcessingStatisticsDao,  sourceDocumentReferenceDao), "statisticsDumper");

        receiverMetaInfoDumper = getContext().system().actorOf(Props.create(ReceiverMetaInfoDumperActor.class, clusterMasterConfig,
                accountantActor,  sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao), "metaInfoDumper");



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
            accountantActor.tell(new ModifyState(downloadConfirmation.getTaskID(), TaskState.PROCESSING), getSelf());
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

        try {
            // save the data in Mongo
            receiverJobDumper.tell(msg,ActorRef.noSender());
            receiverMetaInfoDumper.tell(msg, ActorRef.noSender());
            receiverStatisticsDumper.tell(msg, ActorRef.noSender());
            //update accountant
            accountantActor.tell(new ModifyState(msg.getTaskID(), TaskState.DONE), getSelf());

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
