package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.remote.AssociatedEvent;
import akka.remote.DisassociatedEvent;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.master.accountants.AccountantDispatcherActor;
import eu.europeana.harvester.cluster.master.loaders.JobLoaderMasterActor;
import eu.europeana.harvester.cluster.master.metrics.ProcessingJobStateStatisticsActor;
import eu.europeana.harvester.cluster.master.receivers.ReceiverMasterActor;
import eu.europeana.harvester.cluster.master.senders.JobSenderActor;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.logging.LoggingComponent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClusterMasterActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * The cluster master is split into three separate actors.
     * This reference is reference to an actor which only receives messages from slave.
     */
    private ActorRef receiverActor;

    /**
     * The cluster master is split into three separate actors.
     * This reference is reference to an actor which only loads jobs from MongoDB.
     */
    private ActorRef jobLoaderActor;

    /**
     * The cluster master is split into three separate actors.
     * This reference is reference to an actor which only sends jobs to slaves.
     */
    private ActorRef jobSenderActor;

    /**
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;

    /**
     * A wrapper class for all monitoring data
     */
    private ActorRef monitoringActor;

    /**
     *   every  X seconds computes the number of documents from Mongo which have
     *   the state ERROR, SUCCESS or READY
     */
    private ActorRef processingJobStateStatisticsActor;

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * An object which contains a list of IPs which has to be treated different.
     */
    private final IPExceptions ipExceptions;

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
     * MachineResourceReference DAO object which lets us to read and store data to and from the database.
     */
    private final MachineResourceReferenceDao machineResourceReferenceDao;

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
     * Contains default download limits.
     */
    private final DefaultLimits defaultLimits;

    /**
     * The interval in hours when the master cleans itself and its slaves.
     */
    private final Integer cleanupInterval;

    private final Integer delayForCountingTheStateOfDocuments;

    /**
     * Maps each IP with a boolean which indicates if an IP has jobs in MongoDB or not.
     */
    private final HashMap<String, Boolean> ipsWithJobs = new HashMap<>();



    public ClusterMasterActor (final ClusterMasterConfig clusterMasterConfig,
                               final IPExceptions ipExceptions,
                               final ProcessingJobDao processingJobDao,
                               final MachineResourceReferenceDao machineResourceReferenceDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                               final DefaultLimits defaultLimits,
                               final Integer cleanupInterval,
                               final Integer delayForCountingTheStateOfDocuments) {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                "ClusterMasterActor constructor");

        this.ipExceptions = ipExceptions;
        this.clusterMasterConfig = clusterMasterConfig;
        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.defaultLimits = defaultLimits;
        this.cleanupInterval = cleanupInterval;
        this.delayForCountingTheStateOfDocuments = delayForCountingTheStateOfDocuments;

        this.actorsPerAddress = Collections.synchronizedMap(new HashMap<Address, HashSet<ActorRef>>());
        this.tasksPerAddress = Collections.synchronizedMap(new HashMap<Address, HashSet<String>>());
        this.tasksPerTime = Collections.synchronizedMap(new HashMap<String, DateTime>());
    }

    @Override
    public void preStart() throws Exception {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                "ClusterMasterActor prestart");

        monitoringActor = getContext().system().actorOf(Props.create(ClusterMasterMonitoringActor.class), "monitoring");

        accountantActor = getContext().system().actorOf(Props.create(AccountantDispatcherActor.class), "accountant");

        receiverActor = getContext().system().actorOf(Props.create(ReceiverMasterActor.class, clusterMasterConfig,
                accountantActor, monitoringActor, processingJobDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao
        ), "receiver");

        jobLoaderActor = getContext().system().actorOf(Props.create(JobLoaderMasterActor.class, receiverActor,
                clusterMasterConfig, accountantActor, processingJobDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, machineResourceReferenceDao,
                defaultLimits, ipsWithJobs, ipExceptions), "jobLoader");

        processingJobStateStatisticsActor = getContext().system().actorOf(Props.create(ProcessingJobStateStatisticsActor.class,
                                                                                       sourceDocumentProcessingStatisticsDao,
                                                                                       delayForCountingTheStateOfDocuments),
                                                                          "processingJobStateStatistics");

        jobSenderActor = getContext().system().actorOf(Props.create(JobSenderActor.class, ipExceptions, ipsWithJobs,
        defaultLimits,cleanupInterval, jobLoaderActor,accountantActor, receiverActor), "jobSender");

        final Cluster cluster = Cluster.get(getContext().system());
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                MemberEvent.class, UnreachableMember.class, AssociatedEvent.class);

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(cleanupInterval,
                TimeUnit.HOURS), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());

    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                "ClusterMasterActor prestart");
        getContext().system().stop(jobSenderActor);
        getContext().system().stop(jobLoaderActor);
        getContext().system().stop(receiverActor);
        getContext().system().stop(accountantActor);
        getContext().system().stop(monitoringActor);
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                "ClusterMasterActor poststart");

        getSelf().tell(new LoadJobs(), ActorRef.noSender());
        getSelf().tell(new CheckForTaskTimeout(), ActorRef.noSender());
        getSelf().tell(new Monitor(), ActorRef.noSender());
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof RequestTasks) {
            jobSenderActor.tell(message, getSender());
            return;
        }

        if(message instanceof LoadJobs) {

                jobLoaderActor.tell(message, ActorRef.noSender());
                jobLoaderActor.tell(new LookInDB(), ActorRef.noSender());

            return;
        }

        if(message instanceof Monitor) {
            monitor();
            accountantActor.tell(message, ActorRef.noSender());

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                    TimeUnit.MINUTES), getSelf(), new Monitor(), getContext().system().dispatcher(), getSelf());
            return;
        }

        if(message instanceof CheckForTaskTimeout) {
            //checkForMissedTasks();

            return;
        }

        // cluster events
        if (message instanceof MemberUp) {
            final MemberUp mUp = (MemberUp) message;
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                    "Member is Up: {}", mUp.member());

            return;
        }
        if (message instanceof UnreachableMember) {
            final UnreachableMember mUnreachable = (UnreachableMember) message;
            LOG.info(LoggingComponent.appendAppFields( LoggingComponent.Master.CLUSTER_MASTER),
                    "Member detected as Unreachable: {}", mUnreachable.member());

            return;
        }
        if (message instanceof AssociatedEvent) {
            final AssociatedEvent associatedEvent = (AssociatedEvent) message;

            LOG.info(LoggingComponent.appendAppFields( LoggingComponent.Master.CLUSTER_MASTER),
                    "Member associated: {}", associatedEvent.remoteAddress());

            return;
        }
        if (message instanceof DisassociatedEvent) {
            final DisassociatedEvent disassociatedEvent = (DisassociatedEvent) message;
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                    "Member disassociated: {}", disassociatedEvent.remoteAddress());

            //recoverTasks(disassociatedEvent.remoteAddress());
            return;
        }
        if (message instanceof MemberRemoved) {
            final MemberRemoved mRemoved = (MemberRemoved) message;
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                    "Member is Removed: {}", mRemoved.member());

            //recoverTasks(mRemoved.member().address());

            return;
        }
        if(message instanceof Restart) {
            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(1,
                    TimeUnit.DAYS), getSelf(), "restart for cleanup", getContext().system().dispatcher(), getSelf());

            return;
        }
        if(message instanceof String) {
            LOG.error((String) message);
            throw new NotImplementedException();
        }
        if(message instanceof Clean) {
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                    "Cleaning up ClusterMasterActor and its slaves.");

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(cleanupInterval,
                    TimeUnit.HOURS), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());
            return;
        }
    }

    /**
     * ONLY FOR DEBUG
     */


    // TODO : Refactor this as it polutes the logstash index.
    private void monitor() {
//        LOG.info("Active nodes: {}", tasksPerAddress.size());
//        LOG.info("Actors per node: ");
//        for(final Map.Entry<Address, HashSet<ActorRef>> elem : actorsPerAddress.entrySet()) {
//            LOG.info("Address: {}", elem.getKey());
//            for(final ActorRef actor : elem.getValue()) {
//                LOG.info("\t{}", actor);
//            }
//        }
//
//        LOG.info("Tasks: ");
//        for(final Map.Entry<Address, HashSet<String>> elem : tasksPerAddress.entrySet()) {
//            LOG.info("Address: {}, nr of requests: {}", elem.getKey(), elem.getValue().size());
//        }
        monitoringActor.tell(new Monitor(), ActorRef.noSender());
    }



}
