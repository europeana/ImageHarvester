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
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.remote.AssociatedEvent;
import akka.remote.DisassociatedEvent;
import akka.util.Timeout;
import com.codahale.metrics.MetricRegistry;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.messages.Clean;
import eu.europeana.harvester.cluster.domain.messages.Monitor;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.db.*;
import org.joda.time.DateTime;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class ClusterMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

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
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;

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
     * Used to send messages after each finished job.
     */
    //private final ESBClient esbClient;

    /**
     * A list of tasks generated for a slave.
     */
    private List<RetrieveUrl> tasksToSend;

    /**
     * The interval in hours when the master cleans itself and its slaves.
     */
    private final Integer cleanupInterval;

    /**
     * Maps each IP with a boolean which indicates if an IP has jobs in MongoDB or not.
     */
    private final HashMap<String, Boolean> ipsWithJobs = new HashMap<>();

    private final MetricRegistry metrics;

    private com.codahale.metrics.Timer getJobs;


    private final HashMap< Integer , RequestHolder> jobRequests = new HashMap<>();

    public ClusterMasterActor(final ClusterMasterConfig clusterMasterConfig, final IPExceptions ipExceptions,
                              final ProcessingJobDao processingJobDao,
                              final MachineResourceReferenceDao machineResourceReferenceDao,
                              final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                              final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                              final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                              final DefaultLimits defaultLimits,
                              final Integer cleanupInterval,
                              final MetricRegistry metrics) {
        LOG.info("ClusterMasterActor constructor");

        this.ipExceptions = ipExceptions;
        this.clusterMasterConfig = clusterMasterConfig;
        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.defaultLimits = defaultLimits;
        //this.esbClient = esbClient;
        this.cleanupInterval = cleanupInterval;

        this.accountantActor = getContext().system().actorOf(Props.create(AccountantMasterActor.class), "accountant");

        this.actorsPerAddress = Collections.synchronizedMap(new HashMap<Address, HashSet<ActorRef>>());
        this.tasksPerAddress = Collections.synchronizedMap(new HashMap<Address, HashSet<String>>());
        this.tasksPerTime = Collections.synchronizedMap(new HashMap<String, DateTime>());
        this.metrics = metrics;

    }

    @Override
    public void preStart() throws Exception {
        LOG.info("ClusterMasterActor preStart");

        receiverActor = getContext().system().actorOf(Props.create(ReceiverMasterActor.class, clusterMasterConfig,
                accountantActor, actorsPerAddress, tasksPerAddress, tasksPerTime, processingJobDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao,metrics
        ), "receiver");

        jobLoaderActor = getContext().system().actorOf(Props.create(JobLoaderMasterActor.class, receiverActor,
                clusterMasterConfig, accountantActor, actorsPerAddress, processingJobDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, machineResourceReferenceDao,
                defaultLimits, ipsWithJobs, ipExceptions,metrics), "jobLoader");

        final Cluster cluster = Cluster.get(getContext().system());
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                MemberEvent.class, UnreachableMember.class, AssociatedEvent.class);

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(cleanupInterval,
                TimeUnit.HOURS), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());

        getJobs = metrics.timer(name("ClusterMaster", "Send jobs"));

    }

    @Override
    public void preRestart(Throwable reason, Option<Object> message) throws Exception {
        super.preRestart(reason, message);
        LOG.info("ClusterMasterActor preRestart");

        getContext().system().stop(jobLoaderActor);
        getContext().system().stop(receiverActor);
        getContext().system().stop(accountantActor);
    }

    @Override
    public void postRestart(Throwable reason) throws Exception {
        super.postRestart(reason);
        LOG.info("ClusterMasterActor postRestart");

        getSelf().tell(new LoadJobs(), ActorRef.noSender());
        getSelf().tell(new CheckForTaskTimeout(), ActorRef.noSender());
        getSelf().tell(new Monitor(), ActorRef.noSender());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RequestTasks) {

            final com.codahale.metrics.Timer.Context context = getJobs.time();
            handleRequest(getSender());
            context.stop();

            return;
        }
        if(message instanceof LoadJobs) {
            if( checkRequest(getSender().hashCode())) {
                jobLoaderActor.tell(message, ActorRef.noSender());
                jobLoaderActor.tell(new LookInDB(), ActorRef.noSender());
            }

            return;
        }
        if(message instanceof Monitor) {
            LOG.info("============ Monitor =============");
            accountantActor.tell(new eu.europeana.harvester.cluster.domain.messages.inner.Monitor(), getSelf());
            LOG.info("Active nodes: {}", tasksPerAddress.size());
            monitor();
            LOG.info("===================================");





            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                    TimeUnit.MINUTES), getSelf(), new Monitor(), getContext().system().dispatcher(), getSelf());
            return;
        }
        if(message instanceof CheckForTaskTimeout) {
            checkForMissedTasks();

            return;
        }

        // cluster events
        if (message instanceof MemberUp) {
            final MemberUp mUp = (MemberUp) message;
            LOG.info("Member is Up: {}", mUp.member());

            return;
        }
        if (message instanceof UnreachableMember) {
            final UnreachableMember mUnreachable = (UnreachableMember) message;
            LOG.info("Member detected as Unreachable: {}", mUnreachable.member());

            return;
        }
        if (message instanceof AssociatedEvent) {
            final AssociatedEvent associatedEvent = (AssociatedEvent) message;
            LOG.info("Member associated: {}", associatedEvent.remoteAddress());

            return;
        }
        if (message instanceof DisassociatedEvent) {
            final DisassociatedEvent disassociatedEvent = (DisassociatedEvent) message;
            LOG.info("Member disassociated: {}", disassociatedEvent.remoteAddress());

            recoverTasks(disassociatedEvent.remoteAddress());
            return;
        }
        if (message instanceof MemberRemoved) {
            final MemberRemoved mRemoved = (MemberRemoved) message;
            LOG.info("Member is Removed: {}", mRemoved.member());

            recoverTasks(mRemoved.member().address());

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
            LOG.info("Cleaning up ClusterMasterActor and its slaves.");

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(cleanupInterval,
                    TimeUnit.HOURS), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());
            return;
        }
    }

    private boolean checkRequest(int requester) {

        return true;
    }

    /**
     * Handles the request for new tasks. Sends a predefined number of tasks.
     * @param sender sender actor.
     */
    private void handleRequest(ActorRef sender) {

        final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new CheckIPsWithJobs(ipsWithJobs), timeout);
        Double percentage;
        try {
            percentage = (Double) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("Error: {}", e);
            percentage = 0.0;
        }

        LOG.info("Request tasks from: {}", sender);

        final Long start = System.currentTimeMillis();



        startTasks();

        for(final RetrieveUrl retrieveUrl : tasksToSend) {
            tasksPerTime.put(retrieveUrl.getId(), new DateTime());
        }

        final BagOfTasks bagOfTasks = new BagOfTasks(tasksToSend);
        getSender().tell(bagOfTasks, receiverActor);

        LOG.info("Done with processing the request from: {} in {} seconds. Sent: {}",
                getSender(), (System.currentTimeMillis() - start) / 1000.0, bagOfTasks.getTasks().size());

        LOG.info("Percentage of IPs which has loaded requests: {}% load when it's below: {}",
                percentage, defaultLimits.getMinTasksPerIPPercentage());
        if(percentage < defaultLimits.getMinTasksPerIPPercentage()) {
            //accountantActor.tell(new CleanIPs(), getSelf());
            jobLoaderActor.tell(new LoadJobs(), ActorRef.noSender());
        }
    }

    /**
     * Check if we are allowed to start one or more jobs if yes then starts them.
     */
    private void startTasks() {
        tasksToSend = new ArrayList<>();
        final int maxToSend = defaultLimits.getTaskBatchSize();
        try {
            // Each server is a different case. We treat them different.

            List<String> IPs = new ArrayList(ipsWithJobs.keySet());
            Collections.shuffle(IPs);

            for (final String IP : IPs) {


                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                final Future<Object> future = Patterns.ask(accountantActor, new GetTasksFromIP(IP), timeout);

                List<String> tasksFromIP;
                try {
                    tasksFromIP = (List<String>) Await.result(future, timeout.duration());
                } catch (Exception e) {
                    LOG.error("Error at startTasks->getTasksFromIP: {}", e);
                    continue;
                }


                if (tasksFromIP == null) {
                    continue;
                }


                Boolean success = true;

                // Starts tasks until we have resources or there are tasks to start. (mainly bandwidth)
                while (success && tasksToSend.size() < maxToSend) {
                    success = startOneDownload(tasksFromIP, IP);
               }

                if ( tasksToSend.size() >= maxToSend ) break;


            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Starts one download
     * @param tasksFromIP a list of requests
     * @return - at success true at failure false
     */
    private Boolean startOneDownload(final List<String> tasksFromIP, final String IP) {

        if (! ipExceptions.getIgnoredIPs().contains(IP)) {

            for (final String taskID : tasksFromIP) {

                final Long start = System.currentTimeMillis();
                final boolean isException = ipExceptions.getIps().contains(IP);
                final Long defaultLimit = defaultLimits.getDefaultMaxConcurrentConnectionsLimit();
                final int exceptionLimit = ipExceptions.getMaxConcurrentConnectionsLimit();


                final Timeout timeout = new Timeout(Duration.create(10, TimeUnit.SECONDS));
                Future<Object> future;

                RetrieveUrl retrieveUrl = null;
                GetRetrieveUrl message = new GetRetrieveUrl(taskID, IP, isException, defaultLimit, exceptionLimit);


                future = Patterns.ask(accountantActor, message , timeout);
                try {
                    retrieveUrl = (RetrieveUrl) Await.result(future, timeout.duration());
                } catch (Exception e) {
                    LOG.error("Error at startOneDownload -> getTask: {}", e);
                }
                if (retrieveUrl == null || retrieveUrl.getId().equals("")) {
                    continue;
                }

                tasksToSend.add(retrieveUrl);

                return true;


            }
        }

        return false;
    }


    /**
     * Checks for tasks which was not acknowledged by any slave so they will be reloaded.
     */
    private void checkForMissedTasks() {
        final DateTime currentTime = new DateTime();

        List<String> tasksToRemove = new ArrayList<>();

        try {
            final Map<String, DateTime> tasks = new HashMap<>(tasksPerTime);

            for (final String task : tasks.keySet()) {
                final DateTime timeout =
                        tasks.get(task).plusMillis(clusterMasterConfig.getResponseTimeoutFromSlaveInMillis());
                if (timeout.isBefore(currentTime)) {
                    tasksToRemove.add(task);

                    accountantActor.tell(new ModifyState(task, TaskState.READY), getSelf());
                    accountantActor.tell(new RemoveDownloadSpeed(task), getSelf());
                }
            }

            for(final String task : tasksToRemove) {
                tasksPerTime.remove(task);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        final int period = clusterMasterConfig.getResponseTimeoutFromSlaveInMillis()/1000;
        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(period,
                TimeUnit.SECONDS), getSelf(), new CheckForTaskTimeout(), getContext().system().dispatcher(), getSelf());
    }

    /**
     * Recovers the tasks if an actor system crashes.
     * @param address the address of the actor system.
     */
    private void recoverTasks(final Address address) {
        final HashSet<String> tasks = tasksPerAddress.get(address);
        if(tasks != null) {
            for (final String taskID : tasks) {
                accountantActor.tell(new ModifyState(taskID, TaskState.READY), getSelf());
                accountantActor.tell(new RemoveDownloadSpeed(taskID), getSelf());
            }
        }
        tasksPerAddress.remove(address);
    }

    /**
     * ONLY FOR DEBUG
     */
    private void monitor() {
        LOG.info("Actors per node: ");
        for(final Map.Entry<Address, HashSet<ActorRef>> elem : actorsPerAddress.entrySet()) {
            LOG.info("Address: {}", elem.getKey());
            for(final ActorRef actor : elem.getValue()) {
                LOG.info("\t{}", actor);
            }
        }

        LOG.info("Tasks: ");
        for(final Map.Entry<Address, HashSet<String>> elem : tasksPerAddress.entrySet()) {
            LOG.info("Address: {}, nr of requests: {}", elem.getKey(), elem.getValue().size());
        }
    }

    private class RequestHolder {

        private boolean was_requested;
        private DateTime when;
        private final long delta = 20000L;

        public RequestHolder ( DateTime t) {
            this.was_requested = true;
            this.when = t;
        }

        public void setTime ( DateTime t) {
            this.when = t;
        }

        public DateTime getTime (){
            return when;
        }


    }

}
