package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.messages.Clean;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import org.joda.time.Duration;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

public class JobLoaderMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * The cluster master is splitted into two separate actors.
     * This reference is reference to an actor which only receives messages from slave.
     */
    private final ActorRef receiverActor;

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
     * MachineResourceReference DAO object which lets us to read and store data to and from the database.
     */
    private final MachineResourceReferenceDao machineResourceReferenceDao;

    /**
     * A map which maps each ip with the number of jobs from that ip.
     */
    private Map<String, Integer> ipDistribution;

    /**
     * Contains default download limits.
     */
    private final DefaultLimits defaultLimits;

    /**
     * Maps each IP with a boolean which indicates if an IP has jobs in MongoDB or not.
     */
    private final HashMap<String, Boolean> ipsWithJobs;

    /**
     * An object which contains a list of IPs which has to be treated different.
     */
    private final IPExceptions ipExceptions;

    private final MetricRegistry metrics;

    private Timer loadJobs;

    private long markLoad =0;

    public JobLoaderMasterActor(final ActorRef receiverActor, final ClusterMasterConfig clusterMasterConfig,
                                final ActorRef accountantActor, final Map<Address, HashSet<ActorRef>> actorsPerAddress,
                                final ProcessingJobDao processingJobDao,
                                final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                                final MachineResourceReferenceDao machineResourceReferenceDao,
                                final DefaultLimits defaultLimits,
                                //final HashMap<String, MachineResourceReference> machineResourceReferences,
                                final HashMap<String, Boolean> ipsWithJobs, final IPExceptions ipExceptions, final MetricRegistry metrics ) {
        LOG.info("JobLoaderMasterActor constructor");

        this.receiverActor = receiverActor;
        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.actorsPerAddress = actorsPerAddress;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.defaultLimits = defaultLimits;
        this.ipsWithJobs = ipsWithJobs;
        this.ipExceptions = ipExceptions;
        this.metrics = metrics;
        loadJobs = metrics.timer(name("JobLoaderMaster", "Send jobs"));

        checkForAbandonedJobs();
        getIPDistribution();
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof LoadJobs) {



            if ( markLoad == 0 || System.currentTimeMillis() - markLoad > 300000l ) {
                markLoad = System.currentTimeMillis();
                final Timer.Context context = loadJobs.time();
                try {

                    checkForNewJobs();

                } catch (Exception e) {
                    LOG.error("Error in LoadJobs: " + e.getMessage());
                }
                context.stop();
            } else
                LOG.info("Trying to load jobs too soon");

            return;
        }
        if(message instanceof LookInDB) {
            updateLists();

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                    TimeUnit.MINUTES), getSelf(), new LookInDB(), getContext().system().dispatcher(), getSelf());
        }
        if(message instanceof Clean) {
            LOG.info("Clean JobLoaderMasterActor");
            checkForAbandonedJobs();
            getIPDistribution();

            getSelf().tell(new LoadJobs(), getSelf());
            return;
        }
    }

    private void getIPDistribution() {
        this.ipDistribution = processingJobDao.getIpDistribution();
        LOG.info("IP distribution: ");
        for(Map.Entry<String, Integer> ip : ipDistribution.entrySet()) {
            LOG.info("{}: {}", ip.getKey(), ip.getValue());
        }

        LOG.info("Nr. of machines: {}", ipDistribution.size());
        LOG.info("End of IP distribution");
    }

    /**
     * Updates the list of jobs.
     */
    private void updateLists() {
        try {
            checkForPausedJobs();
            checkForResumedJobs();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    /**
     * Checks if there were added any new jobs in the db
     */
    private void checkForNewJobs() {
        final int taskSize = getAllTasks();

        if (taskSize<clusterMasterConfig.getMaxTasksInMemory() ) {
            //don't load for IPs that are overloaded
            ArrayList<String> noLoadIPs = getOverLoadedIPList(1000);
            HashMap < String, Integer > tempDistribution = new HashMap<>(ipDistribution);
            if ( noLoadIPs != null ) {
                for (String ip : noLoadIPs ) {
                    if (tempDistribution.containsKey(ip))
                        tempDistribution.remove(ip);
                }
            }



            LOG.info("========== Looking for new jobs from MongoDB ==========");
            Long start = System.currentTimeMillis();
            LOG.info("#IPs with tasks: temp {}, all: {}", tempDistribution.size(), ipDistribution.size());


            final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
            final List<ProcessingJob> all =
                    processingJobDao.getDiffusedJobsWithState(JobState.READY, page, tempDistribution, ipsWithJobs);
            LOG.info("Done with loading jobs in {} seconds. Creating tasks from {} jobs.",
                    (System.currentTimeMillis() - start) / 1000.0, all.size());
            start = System.currentTimeMillis();

            final List<String> resourceIds = new ArrayList<>();
            for (final ProcessingJob job : all) {
                if (job == null || job.getTasks() == null) {
                    continue;
                }
                for (final ProcessingJobTaskDocumentReference task : job.getTasks()) {
                    final String resourceId = task.getSourceDocumentReferenceID();
                    resourceIds.add(resourceId);
                }
            }
            final List<SourceDocumentReference> sourceDocumentReferences = sourceDocumentReferenceDao.read(resourceIds);
            final Map<String, SourceDocumentReference> resources = new HashMap<>();
            for (SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
                resources.put(sourceDocumentReference.getId(), sourceDocumentReference);
            }
            LOG.info("Done with loading resources in {} seconds. Creating tasks from {} resources.",
                    (System.currentTimeMillis() - start) / 1000.0, resources.size());

            int i = 0;
            for (final ProcessingJob job : all) {
                try {

                        i++;
                        if (i >= 500) {
                            LOG.info("Done with another 500 jobs out of {}", all.size());
                            i = 0;
                        }
                        addJob(job, resources);

                } catch (Exception e) {
                    LOG.error("JobLoaderMasterActor, while loading job: {} -> {}", job.getId(), e.getMessage());
                }
            }
        }
    }


    private int getAllTasks(){
        final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new GetNumberOfTasks(), timeout);
        int tasks = 0;
        try {
            tasks = (int) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("getAllTasks Error: {}", e);
        }

        return tasks;
    }

    private ArrayList<String> getOverLoadedIPList( int threshold){
        final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(30, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new GetOverLoadedIPs(threshold), timeout);
        ArrayList<String> ips = null;
        try {
            ips = (ArrayList<String>) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("OverloadedIPs Error: {}", e);
        }

        return ips;
    }

    /**
     * Checks if any job was stopped by a client.
     */
    private void checkForPausedJobs() {
        LOG.info("========= Looking for paused jobs from MongoDB ========");
        final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
        final List<ProcessingJob> all = processingJobDao.getJobsWithState(JobState.PAUSE, page);

        for (final ProcessingJob job : all) {
            for(Map.Entry elem : actorsPerAddress.entrySet()) {
                for(final ActorRef actor : (HashSet<ActorRef>)elem.getValue()) {
                    actor.tell(new ChangeJobState(JobState.PAUSE, job.getId()), receiverActor);
                }
            }

            final ProcessingJob newProcessingJob = job.withState(JobState.PAUSED);
            processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

            accountantActor.tell(new PauseTasks(job.getId()), getSelf());
        }
    }

    /**
     * Checks if any job was started by a client.
     */
    private void checkForResumedJobs() {
        LOG.info("======== Looking for resumed jobs from MongoDB ========");
        final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
        final List<ProcessingJob> all = processingJobDao.getJobsWithState(JobState.RESUME, page);

        final List<String> resourceIds = new ArrayList<>();
        for(final ProcessingJob job : all) {
            for(final ProcessingJobTaskDocumentReference task : job.getTasks()) {
                final String resourceId = task.getSourceDocumentReferenceID();
                resourceIds.add(resourceId);
            }
        }
        final Map<String, SourceDocumentReference> resources = new HashMap<>();
        if(resourceIds.size() != 0) {
            final List<SourceDocumentReference> sourceDocumentReferences = sourceDocumentReferenceDao.read(resourceIds);
            for (SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
                resources.put(sourceDocumentReference.getId(), sourceDocumentReference);
            }
        }

        for(final ProcessingJob job : all) {
            for(Map.Entry elem : actorsPerAddress.entrySet()) {
                for(final ActorRef actor : (HashSet<ActorRef>)elem.getValue()) {
                    actor.tell(new ChangeJobState(JobState.RESUME, job.getId()), receiverActor);
                }
            }

            final ProcessingJob newProcessingJob = job.withState(JobState.RUNNING);
            processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

            final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
            final Future<Object> future = Patterns.ask(accountantActor, new IsJobLoaded(job.getId()), timeout);
            Boolean isLoaded = false;
            try {
                isLoaded = (Boolean) Await.result(future, timeout.duration());
            } catch (Exception e) {
                LOG.error("Error in checkForResumedJobs->IsJobLoaded: {}", e);
            }

            if(isLoaded) {
                accountantActor.tell(new ResumeTasks(job.getId()), getSelf());
            } else {
                addJob(job, resources);
            }
        }
    }

    /**
     * Adds a job and its tasks to our evidence.
     * @param job the ProcessingJob object
     */
    private void addJob(final ProcessingJob job, final Map<String, SourceDocumentReference> resources) {
        final List<ProcessingJobTaskDocumentReference> tasks = job.getTasks();

        final ProcessingJob newProcessingJob = job.withState(JobState.RUNNING);
        processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

        final List<String> taskIDs = new ArrayList<>();
        for(final ProcessingJobTaskDocumentReference task : tasks) {
            final String ID = processTask(job, task, resources);
            if(ID != null) {
                taskIDs.add(ID);
            }
        }

        if (tasks.size() > 10 )
            LOG.info("Loaded {} tasks for jobID {} on IP {}",tasks.size(),job.getId(), job.getIpAddress());

        accountantActor.tell(new AddTasksToJob(job.getId(), taskIDs), getSelf());
    }

    /**
     * Loads the task and all needed resources for that task.
     * @param job the job which contains the task
     * @param task the concrete task to load
     * @return generated task ID
     */
    private String processTask(final ProcessingJob job, final ProcessingJobTaskDocumentReference task,
                               final Map<String, SourceDocumentReference> resources) {
        final String sourceDocId = task.getSourceDocumentReferenceID();

        final SourceDocumentReference sourceDocumentReference = resources.get(sourceDocId);
        if(sourceDocumentReference == null) {
            return null;
        }

        final String ipAddress = job.getIpAddress();
        final Long speedLimitPerLink = getSpeed(ipAddress);

        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100l),
                speedLimitPerLink, speedLimitPerLink,  Duration.millis(600000l),5000l /* 5 KB / sec */, 0l, true, task.getTaskType(),
                defaultLimits.getConnectionTimeoutInMillis(), defaultLimits.getMaxNrOfRedirects());

        final RetrieveUrl retrieveUrl = new RetrieveUrl(sourceDocumentReference.getUrl(), httpRetrieveConfig,
                job.getId(), task.getSourceDocumentReferenceID(),
                getHeaders(task.getTaskType(), sourceDocumentReference), task, ipAddress);

        List<String> tasksFromIP = null;
        final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new GetTasksFromIP(ipAddress), timeout);
        try {
            tasksFromIP = (List<String>) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("Error in checkForResumedJobs->GetTasksFromIP: {}", e);
        }

        if(tasksFromIP == null) {
            tasksFromIP = new ArrayList<>();
        } else {
            if(tasksFromIP.contains(retrieveUrl.getId())) {
                return null;
            }
        }
        tasksFromIP.add(retrieveUrl.getId());

        accountantActor.tell(new AddTasksToIP(ipAddress, tasksFromIP), getSelf());
        accountantActor.tell(new AddTask(retrieveUrl.getId(), new Pair<>(retrieveUrl, TaskState.READY)), getSelf());

        return retrieveUrl.getId();
    }

    /**
     * Calculates the allowed download speed.
     * @param ipAddress
     * @return the download speed
     */
    private Long getSpeed(final String ipAddress) {
        Long speedLimitPerLink;
        if(ipExceptions.getIps().contains(ipAddress)) {
            speedLimitPerLink = defaultLimits.getDefaultBandwidthLimitReadInBytesPerSec() /
                    ipExceptions.getMaxConcurrentConnectionsLimit();
        } else {
            speedLimitPerLink = defaultLimits.getDefaultBandwidthLimitReadInBytesPerSec() /
                    defaultLimits.getDefaultMaxConcurrentConnectionsLimit();
        }

        return speedLimitPerLink;
    }

    /**
     * Returns the headers of a source document if we already retrieved that at least once.
     * @param documentReferenceTaskType task type
     * @param newDoc source document object
     * @return list of headers
     */
    private Map<String, String> getHeaders(final DocumentReferenceTaskType documentReferenceTaskType,
                                           final SourceDocumentReference newDoc) {
        Map<String, String> headers = null;

        if(documentReferenceTaskType == null) {
            return null;
        }

        if((DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD).equals(documentReferenceTaskType)) {
            final String statisticsID = newDoc.getLastStatsId();
            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatisticsDao.read(statisticsID);
            try {
                headers = sourceDocumentProcessingStatistics.getHttpResponseHeaders();
            } catch (Exception e) {
                headers = new HashMap<>();
            }
        }

        return headers;
    }

    /**
     * Checks if any job was started but due to an issue of this node it has been abandoned.
     */
    private void checkForAbandonedJobs() {
        LOG.info("======== Looking for abandoned jobs from MongoDB ========");
        final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
        List<ProcessingJob> all;
        do {
            all = processingJobDao.getJobsWithState(JobState.RUNNING, page);

            for (final ProcessingJob job : all) {
                final ProcessingJob newProcessingJob = job.withState(JobState.READY);
                processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());
            }
        } while (all.size() != 0);

        do {
            all = processingJobDao.getJobsWithState(JobState.LOADED, page);

            for (final ProcessingJob job : all) {
                final ProcessingJob newProcessingJob = job.withState(JobState.READY);
                processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());
            }
        } while (all.size() != 0);

        LOG.info("======== Done with abandoned jobs ========");
    }

}
