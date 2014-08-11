package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.JobConfigs;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.servicebus.client.ESBClient;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.lang.Exception;
import java.net.InetAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class ClusterMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * Number of sent tasks.
     */
    private int NR_TASKS = 0; //only for debug

    /**
     * The number of active nodes. If there is no active node the cluster master waits with the sending of tasks.
     */
    private int activeNodes = 0;

    /**
     * The cluster master is splitted into two separate actors.
     * This reference is reference to an actor which only recieves messages from slave.
     */
    private ActorRef receiverActor;

    /**
     * The routers reference. We send all the messages to the router actor and then he decides the next step.
     */
    private final ActorRef routerActor;

    /**
     * Different configs for different types of tasks.
     */
    private final JobConfigs jobConfigs;

    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * A map with all jobs which maps each job with an other map. The inner map contains all the links with their state.
     */
    private final Map<String, Map<String,JobState>> allJobs;

    /**
     * Map of urls and their task type/job(link check, conditional or unconditional download).
     */
    private final Map<String, Map<String, DocumentReferenceTaskType>> taskTypeOfDoc;

    /**
     * Map which stores a list of jobs for each ipAddress.
     */
    private final Map<String, List<Pair<String,String>>> linksPerIpAddress;

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
     * Maps each url with an other map. The inner map maps jobs with speed.
     * (It is necessary because many jobs can have the same url in their task list)
     */
    private final HashMap<String, HashMap<String, Long>> speedPerUrl = new HashMap<String, HashMap<String, Long>>();

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
     * LinkCheckLimits DAO object which lets us to read and store data to and from the database.
     */
    private final LinkCheckLimitsDao linkCheckLimitsDao;

    /**
     * Contains default download limits.
     */
    private final DefaultLimits defaultLimits;

    /**
     * Used to send messages after each finished job.
     */
    private final ESBClient esbClient;

    public ClusterMasterActor(final JobConfigs jobConfigs, final ClusterMasterConfig clusterMasterConfig,
                              final ProcessingJobDao processingJobDao,
                              final MachineResourceReferenceDao machineResourceReferenceDao,
                              final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                              final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                              final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                              final LinkCheckLimitsDao linkCheckLimitsDao, final ActorRef routerActor,
                              final DefaultLimits defaultLimits, final ESBClient esbClient) {
        this.jobConfigs = jobConfigs;
        this.clusterMasterConfig = clusterMasterConfig;
        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.linkCheckLimitsDao = linkCheckLimitsDao;
        this.routerActor = routerActor;
        this.defaultLimits = defaultLimits;
        this.esbClient = esbClient;

        this.allJobs = Collections.synchronizedMap(new HashMap<String, Map<String, JobState>>());
        this.taskTypeOfDoc = Collections.synchronizedMap(new HashMap<String, Map<String, DocumentReferenceTaskType>>());
        this.linksPerIpAddress = Collections.synchronizedMap(new HashMap<String, List<Pair<String, String>>>());
        this.actorsPerAddress = Collections.synchronizedMap(new HashMap<Address, HashSet<ActorRef>>());
        this.tasksPerAddress = Collections.synchronizedMap(new HashMap<Address, HashSet<Pair<String, String>>>());
        this.tasksPerTime = Collections.synchronizedMap(new HashMap<Pair<String, String>, DateTime>());
    }

    @Override
    public void preStart() throws Exception {
        receiverActor = getContext().system().actorOf(Props.create(ReceiverClusterActor.class, clusterMasterConfig,
                allJobs, actorsPerAddress, tasksPerAddress, tasksPerTime, taskTypeOfDoc, processingJobDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, sourceDocumentReferenceMetaInfoDao,
                esbClient));

        final Cluster cluster = Cluster.get(getContext().system());
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(),
                MemberEvent.class, UnreachableMember.class);

        getContext().setReceiveTimeout(scala.concurrent.duration.Duration.create(
                clusterMasterConfig.getReceiveTimeoutInterval().getStandardSeconds(), TimeUnit.SECONDS));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RecoverAbandonedJobs) {
            checkForAbandonedJobs();

            return;
        }
        if(message instanceof LookInDB) {
            LOG.info("============== Looking for jobs from MongoDB =============="); //only for debug
            updateLists();
            monitor(); //only for debug
            LOG.info("==========================================================="); //only for debug

            return;
        }
        if(message instanceof StartTasks) {
            LOG.info("============ Starting tasks ============="); //only for debug
            startTasks();
            LOG.info("========================================="); //only for debug
            LOG.debug("Sent: {}, active nodes: {}",  NR_TASKS, activeNodes); //only for debug

            return;
        }
        if(message instanceof CheckForTaskTimeout) {
            checkForMissedTasks();

            return;
        }
        if(message instanceof RetrieveUrl) {
            final RetrieveUrl retrieveUrl = (RetrieveUrl) message;
            final Pair<String, String> task =
                    new Pair<String, String>(retrieveUrl.getJobId(), retrieveUrl.getReferenceId());
            tasksPerTime.put(task, new DateTime());

            routerActor.tell(message, receiverActor);
            NR_TASKS++;

            return;
        }

        // cluster events
        if (message instanceof MemberUp) {
            final MemberUp mUp = (MemberUp) message;
            LOG.info("Member is Up: {}", mUp.member());

            final boolean self =
                    mUp.member().address().equals(Cluster.get(getContext().system()).selfAddress());

            if(actorsPerAddress.containsKey(mUp.member().address()) && !self) {
                final HashSet<ActorRef> actorRefs = actorsPerAddress.get(mUp.member().address());

                for(final ActorRef actorRef : actorRefs) {
                    actorRef.tell(new Clean(), receiverActor);
                }
            }

            if(!self) {
                activeNodes++;
            }

            return;
        }
        if (message instanceof UnreachableMember) {
            final UnreachableMember mUnreachable = (UnreachableMember) message;
            LOG.info("Member detected as Unreachable: {}", mUnreachable.member());

            return;
        }
        if (message instanceof MemberRemoved) {
            final MemberRemoved mRemoved = (MemberRemoved) message;
            LOG.info("Member is Removed: {}", mRemoved.member());

            recoverTasks(mRemoved.member().address());
            activeNodes--;

            return;
        }
    }

    //only for debug
    private void monitor() {
        LOG.debug("Actors per node: ");
        for(final Map.Entry elem : actorsPerAddress.entrySet()) {
            LOG.debug("Address: {}", elem.getKey());
            for(final ActorRef actor : (HashSet<ActorRef>)elem.getValue()) {
                LOG.debug("\t, {}", actor);
            }
        }

        LOG.debug("Tasks: ");
        for(final Map.Entry elem : tasksPerAddress.entrySet()) {
            LOG.debug("Address: {} \nnr of tasks: ", elem.getKey(),
                    ((HashSet<Pair<String, String>>)elem.getValue()).size());
        }
    }

    /**
     * Updates the list of jobs.
     */
    private void updateLists() {
        try {
            checkForNewJobs();
            checkForPausedJobs();
            checkForResumedJobs();
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        final int period = (int)clusterMasterConfig.getJobsPollingInterval().getStandardSeconds();
        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(period,
                TimeUnit.SECONDS), getSelf(), new LookInDB(), getContext().system().dispatcher(), getSelf());
    }

    /**
     * Checks if there were added any new jobs in the db
     */
    private void checkForNewJobs() {
        LOG.info("========== Looking for new jobs from MongoDB ==========");
        final Page page = new Page(0, clusterMasterConfig.getMaxJobsPerIteration());
        final List<ProcessingJob> all = processingJobDao.getJobsWithState(JobState.READY, page);

        final ExecutorService service = Executors.newSingleThreadExecutor();
        for(final ProcessingJob job : all) {
            if(!allJobs.containsKey(job.getId())) {
                addJob(job, service);
            }
        }

        service.shutdown();
    }

    /**
     * Checks if any job was stopped by a client.
     */
    private void checkForPausedJobs() {
        LOG.info("========= Looking for paused jobs from MongoDB ========");
        final Page page = new Page(0, clusterMasterConfig.getMaxJobsPerIteration());
        final List<ProcessingJob> all = processingJobDao.getJobsWithState(JobState.PAUSE, page);

        for(final ProcessingJob job : all) {
            for(Map.Entry elem : actorsPerAddress.entrySet()) {
                for(final ActorRef actor : (HashSet<ActorRef>)elem.getValue()) {
                   actor.tell(new ChangeJobState(JobState.PAUSE, job.getId()), receiverActor);
                }
            }

            final ProcessingJob newProcessingJob = job.withState(JobState.PAUSED);
            processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

            if(allJobs.containsKey(job.getId())) {
                final Map<String, JobState> urlsWithState = allJobs.get(job.getId());
                for (final String url : urlsWithState.keySet()) {
                    if(!(urlsWithState.get(url).equals(JobState.FINISHED) ||
                            urlsWithState.get(url).equals(JobState.ERROR))) {
                        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                                sourceDocumentProcessingStatisticsDao
                                        .findBySourceDocumentReferenceAndJobId(url, job.getId());
                        final SourceDocumentProcessingStatistics updatedProcessingStatistics =
                                sourceDocumentProcessingStatistics.withState(ProcessingState.PAUSED);
                        sourceDocumentProcessingStatisticsDao.update(updatedProcessingStatistics,
                                clusterMasterConfig.getWriteConcern());

                        urlsWithState.put(url, JobState.PAUSE);
                    }
                }
                allJobs.put(job.getId(), urlsWithState);
            }
        }
    }

    /**
     * Checks if any job was started by a client.
     */
    private void checkForResumedJobs() {
        LOG.info("======== Looking for resumed jobs from MongoDB ========");
        final Page page = new Page(0, clusterMasterConfig.getMaxJobsPerIteration());
        final List<ProcessingJob> all = processingJobDao.getJobsWithState(JobState.RESUME, page);

        final ExecutorService service = Executors.newSingleThreadExecutor();
        for(final ProcessingJob job : all) {
            for(Map.Entry elem : actorsPerAddress.entrySet()) {
                for(final ActorRef actor : (HashSet<ActorRef>)elem.getValue()) {
                    actor.tell(new ChangeJobState(JobState.RESUME, job.getId()), receiverActor);
                }
            }

            final ProcessingJob newProcessingJob = job.withState(JobState.RUNNING);
            processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

            if(allJobs.containsKey(job.getId())) {
                final Map<String, JobState> urlsWithState = allJobs.get(job.getId());
                for (final String url : urlsWithState.keySet()) {
                    if(urlsWithState.get(url).equals(JobState.PAUSE)) {
                        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                                sourceDocumentProcessingStatisticsDao
                                        .findBySourceDocumentReferenceAndJobId(url, job.getId());
                        final SourceDocumentProcessingStatistics updatedProcessingStatistics =
                                sourceDocumentProcessingStatistics.withState(ProcessingState.READY);
                        sourceDocumentProcessingStatisticsDao.update(updatedProcessingStatistics,
                                clusterMasterConfig.getWriteConcern());

                        urlsWithState.put(url, JobState.READY);
                    }
                }
                allJobs.put(job.getId(), urlsWithState);
            } else {
                addJob(job, service);
            }
        }
        service.shutdown();
    }

    /**
     * Checks if any job was started but due to an issue of this node it has been abandoned.
     */
    private void checkForAbandonedJobs() {
        LOG.info("======== Looking for abandoned jobs from MongoDB ========");
        final Page page = new Page(0, clusterMasterConfig.getMaxJobsPerIteration());
        final List<ProcessingJob> all = processingJobDao.getJobsWithState(JobState.RUNNING, page);

        final ExecutorService service = Executors.newSingleThreadExecutor();
        for(final ProcessingJob job : all) {
            addJob(job, service);
        }
        service.shutdown();
    }

    /**
     * Adds a job and its tasks to our evidence.
     * @param job the ProcessingJob object
     * @param service executor service for timed method calls.
     */
    private void addJob(final ProcessingJob job, final ExecutorService service) {
        final List<ProcessingJobTaskDocumentReference> tasks = job.getTasks();
        final Map<String, JobState> urlsWithState = Collections.synchronizedMap(new HashMap<String, JobState>());

        final ProcessingJob newProcessingJob = job.withState(JobState.RUNNING);
        processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

        for(final ProcessingJobTaskDocumentReference task : tasks) {
            final String sourceDocId = task.getSourceDocumentReferenceID();
            if(allJobs.get(job.getId()) != null && allJobs.get(job.getId()).get(sourceDocId) != null) {
                LOG.error("Duplicated url in the job's task list.");
                continue;
            }
            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(sourceDocId, job.getId());
            if(sourceDocumentProcessingStatistics != null &&
                    (sourceDocumentProcessingStatistics.getState().equals(ProcessingState.SUCCESS) ||
                            sourceDocumentProcessingStatistics.getState().equals(ProcessingState.ERROR))) {

                continue;
            }

            final SourceDocumentReference sourceDocumentReference = sourceDocumentReferenceDao.read(sourceDocId);
            if(sourceDocumentReference == null) {
                continue;
            }
            String ipAddress = sourceDocumentReference.getIpAddress();
            if(ipAddress == null) {
                ipAddress = getIPAddress(service, sourceDocumentReference, job);
            }

            if(ipAddress != null) {
                List<Pair<String, String>> links = linksPerIpAddress.get(ipAddress);
                if(links == null) {
                    links = new ArrayList<Pair<String, String>>();
                }
                links.add(new Pair<String, String>(job.getId(), sourceDocId));

                linksPerIpAddress.put(ipAddress, links);

                urlsWithState.put(sourceDocId, JobState.READY);
            } else {
                urlsWithState.put(sourceDocId, JobState.ERROR);
            }
            Map<String, DocumentReferenceTaskType> docTypes = taskTypeOfDoc.get(sourceDocId);
            if(docTypes == null) {
                docTypes = new HashMap<String, DocumentReferenceTaskType>();
                docTypes.put(job.getId(), task.getTaskType());
            }
            taskTypeOfDoc.put(sourceDocId, docTypes);
        }

        allJobs.put(job.getId(), urlsWithState);
    }

    /**
     * Gets the ip address of a source document.
     * @param service executor service for timed method calls.
     * @param sourceDocumentReference SourceDocumentReference object
     * @param job the job which contains a task with the given source document
     * @return - ip address
     */
    private String getIPAddress(final ExecutorService service, final SourceDocumentReference sourceDocumentReference,
                                final ProcessingJob job) {
        final String sourceDocId = sourceDocumentReference.getId();
        String ipAddress;
        String exception = "";
        final Future<String> future = service.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                final InetAddress address = InetAddress.getByName(new URL(sourceDocumentReference.getUrl()).getHost());

                return address.getHostAddress();
            }
        });

        try {
            ipAddress = future.get(defaultLimits.getConnectionTimeoutInMillis(), TimeUnit.MILLISECONDS);
        }
        catch(TimeoutException e) {
            ipAddress = null;
            exception = e.toString();
        } catch (InterruptedException e) {
            ipAddress = null;
            exception = e.toString();
        } catch (ExecutionException e) {
            ipAddress = null;
            exception = e.toString();
        }

        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics;
        if(ipAddress != null) {
            SourceDocumentReference updatedSourceDocumentReference =
                    sourceDocumentReference.withIPAddress(ipAddress);
            sourceDocumentReferenceDao.update(updatedSourceDocumentReference,
                    clusterMasterConfig.getWriteConcern());
            sourceDocumentProcessingStatistics = new SourceDocumentProcessingStatistics(new Date(), new Date(),
                    ProcessingState.READY, sourceDocumentReference.getReferenceOwner(), sourceDocumentReference.getId(),
                    job.getId(), null, null, null, null, null, null, null, null, null);
        } else {
            sourceDocumentProcessingStatistics = new SourceDocumentProcessingStatistics(new Date(), new Date(),
                    ProcessingState.ERROR, sourceDocumentReference.getReferenceOwner(), sourceDocumentReference.getId(),
                    job.getId(), -1, "", null, null, null, null, "", null, "In ClusterMasterActor: \n" + exception);
        }

        if(sourceDocumentProcessingStatisticsDao
                .findBySourceDocumentReferenceAndJobId(sourceDocId, job.getId()) == null) {
            sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics,
                    clusterMasterConfig.getWriteConcern());
        } else {
            sourceDocumentProcessingStatisticsDao.update(sourceDocumentProcessingStatistics,
                    clusterMasterConfig.getWriteConcern());
        }

        return ipAddress;
    }

    /**
     * Checks for tasks which was not acknowledged by any slave so they will be reloaded.
     */
    private void checkForMissedTasks() {
        final DateTime currentTime = new DateTime();

        List<Pair<String, String>> tasksToRemove = new ArrayList<Pair<String, String>>();
        final Map<Pair<String, String>, DateTime> tasks = tasksPerTime;

        try {
            for(final Pair<String, String> task : tasks.keySet()) {
                final DateTime timeout =
                        tasksPerTime.get(task).plusMillis(clusterMasterConfig.getResponseTimeoutFromSlaveInMillis());
                if(timeout.isBefore(currentTime)) {
                    tasksToRemove.add(task);
                    allJobs.get(task.getKey()).put(task.getValue(), JobState.READY);
                    speedPerUrl.get(task.getValue()).remove(task.getValue());
                }
            }

            for(final Pair<String, String> task : tasksToRemove) {
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
     * Check if we are allowed to start one or more jobs if yes then starts them.
     */
    private void startTasks() {
        int ipsWithUnfinishedLinks = 0;
        boolean firstPerIP;

        final int period = (int)clusterMasterConfig.getTaskStartingInterval().getStandardSeconds();
        float adaptedPeriod = period;

        int finished = 0, error = 0; //only for debug

        try {
            // Each server is a different case. We treat them different.
            for (final Map.Entry entry : linksPerIpAddress.entrySet()) {
                firstPerIP = true;
                MachineResourceReference machineResourceReference =
                        machineResourceReferenceDao.read((String) entry.getKey());
                if(machineResourceReference == null) {
                    machineResourceReference = new MachineResourceReference((String) entry.getKey(), null,
                            null, defaultLimits.getDefaultBandwidthLimitReadInBytesPerSec(),
                            defaultLimits.getDefaultMaxConcurrentConnectionsLimit());
                }

                List<ReferenceOwner> referenceOwners = machineResourceReference.getReferenceOwnerList();
                if(referenceOwners == null) {
                    referenceOwners = new ArrayList<ReferenceOwner>();
                }

                int runningDownloads = 0;
                Long consumedBandwidth = 0l;

                final List<Pair<String, String>> linkPerJob = (List<Pair<String, String>>) entry.getValue();

                // Calculates the occupied bandwidth.
                for (final Pair<String, String> task : linkPerJob) {
                    final JobState jobsLinkState = allJobs.get(task.getKey()).get(task.getValue());
                    if(jobsLinkState.equals(JobState.RUNNING)) {
                        runningDownloads++;
                        consumedBandwidth += speedPerUrl.get(task.getValue()).get(task.getKey());
                    }
                    //only for debug
                    if(jobsLinkState.equals(JobState.FINISHED)) {
                        finished++;
                    } else if(jobsLinkState.equals(JobState.ERROR)) {
                        error++;
                    }
                    if(jobsLinkState.equals(JobState.READY) && firstPerIP) {
                        firstPerIP = false;
                        ipsWithUnfinishedLinks++;
                    }
                }

                // Maximum speed per link
                final Long speedLimitPerLink = machineResourceReference.getBandwidthLimitReadInBytesPerSec() /
                                machineResourceReference.getMaxConcurrentConnectionsLimit();

                boolean changed = false;
                // Starts tasks until we have resources. (mainly bandwidth)
                while(runningDownloads < machineResourceReference.getMaxConcurrentConnectionsLimit() &&
                        ((machineResourceReference.getBandwidthLimitReadInBytesPerSec() - consumedBandwidth) >=
                                speedLimitPerLink) &&
                        activeNodes > 0) {

                    final ReferenceOwner referenceOwner = startOneDownload(linkPerJob, speedLimitPerLink);
                    if(referenceOwner == null) {
                        break;
                    }

                    if(!referenceOwners.contains(referenceOwner)) {
                        referenceOwners.add(referenceOwner);
                        changed = true;
                    }

                    runningDownloads++;
                    consumedBandwidth += speedLimitPerLink;
                }

                if(changed) {
                    machineResourceReference = machineResourceReference.withReferenceOwnerList(referenceOwners);
                    machineResourceReferenceDao.
                            createOrModify(machineResourceReference, clusterMasterConfig.getWriteConcern());
                }
            }
            LOG.debug("Finished: {}, Error: {}", finished, error); //only for debug

            if(linksPerIpAddress.size() != 0 || ipsWithUnfinishedLinks != 0) {
                adaptedPeriod = ipsWithUnfinishedLinks * period / (float)linksPerIpAddress.size();
                LOG.debug("{} : {} {}", period, (int)adaptedPeriod,
                        ipsWithUnfinishedLinks/(float)linksPerIpAddress.size()); //only for debug
            }
            if(adaptedPeriod < 2) {
                adaptedPeriod = 2;
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create((int)adaptedPeriod,
                TimeUnit.SECONDS), getSelf(), new StartTasks(), getContext().system().dispatcher(), getSelf());
    }

    /**
     * Starts one download
     * @param linkPerJob a map of links with jobs
     * @param speed allowed maximum speed
     * @return - at success a ReferenceOwner object at failure null
     */
    private ReferenceOwner startOneDownload(final List<Pair<String, String>> linkPerJob, final Long speed) {
        for (final Pair<String, String> task : linkPerJob) {
            final String jobId = task.getKey();

            if(allJobs.get(jobId).get(task.getValue()).equals(JobState.READY)) {
                final String sourceDocId = task.getValue();
                allJobs.get(jobId).put(sourceDocId, JobState.RUNNING);

                HashMap<String, Long> speeds = speedPerUrl.get(sourceDocId);
                if(speeds == null) {
                    speeds = new HashMap<String, Long>();
                }

                final DocumentReferenceTaskType documentReferenceTaskType = taskTypeOfDoc.get(sourceDocId).get(jobId);
                final SourceDocumentReference newDoc = sourceDocumentReferenceDao.read(sourceDocId);
                final List<ResponseHeader> headers = getHeaders(documentReferenceTaskType, newDoc);

                final HttpRetrieveConfig httpRetrieveConfig =
                        new HttpRetrieveConfig(Duration.millis(100), speed, speed, Duration.ZERO, 0l, true,
                                documentReferenceTaskType, defaultLimits.getConnectionTimeoutInMillis(),
                                defaultLimits.getMaxNrOfRedirects());
                getSelf().tell(new RetrieveUrl(newDoc.getUrl(), httpRetrieveConfig, jobId, newDoc.getId(), headers,
                        documentReferenceTaskType, jobConfigs), receiverActor);

                speeds.put(jobId, speed);
                speedPerUrl.put(sourceDocId, speeds);

                return newDoc.getReferenceOwner();
            }
        }

        return null;
    }

    /**
     * Returns the headers of a source document if we already retrieved that at least once.
     * @param documentReferenceTaskType task type
     * @param newDoc source document object
     * @return list of headers
     */
    private List<ResponseHeader> getHeaders(DocumentReferenceTaskType documentReferenceTaskType,
                                            SourceDocumentReference newDoc) {
        List<ResponseHeader> headers = null;

        if(documentReferenceTaskType == null) {
            return headers;
        }

        if(documentReferenceTaskType.equals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD)) {
            final String statisticsID = newDoc.getLastStatsId();
            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatisticsDao.read(statisticsID);
            try {
                headers = sourceDocumentProcessingStatistics.getHttpResponseHeaders();
            } catch (Exception e) {
                headers = new ArrayList<ResponseHeader>();
            }
        }

        return headers;
    }

    /**
     * Recovers the tasks if an actor system crashes.
     * @param address the address of the actor system.
     */
    private void recoverTasks(final Address address) {
        final HashSet<Pair<String, String>> tasks = tasksPerAddress.get(address);

        for(final Pair<String, String> task : tasks) {
            String jobId = task.getKey();
            String sourceDocId = task.getValue();

            allJobs.get(jobId).put(sourceDocId, JobState.READY);
            speedPerUrl.get(sourceDocId).remove(jobId);
        }
        tasksPerAddress.remove(address);
    }

}
