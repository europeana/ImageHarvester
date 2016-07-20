package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.AddTask;
import eu.europeana.harvester.cluster.domain.messages.inner.GetNumberOfTasks;
import eu.europeana.harvester.cluster.domain.messages.inner.GetOverLoadedIPs;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.cluster.master.limiter.domain.ChangeMaxAvailableSlotsRequest;
import eu.europeana.harvester.cluster.master.metrics.MasterMetrics;
import eu.europeana.harvester.db.interfaces.MachineResourceReferenceDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class JobLoaderExecutorHelper {

    /**
     * Checks if there were added any new jobs in the db
     */
    public static void checkForNewJobs(ClusterMasterConfig clusterMasterConfig, Map<String, Integer> ipDistribution,
                                       HashMap<String, Boolean> ipsWithJobs, ActorRef accountantActor,ActorRef limiterActor, ProcessingJobDao processingJobDao,
                                       SourceDocumentReferenceDao SourceDocumentReferenceDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                       final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                       Logger LOG) {

        checkForNewJobsByPriority(JobPriority.NORMAL, clusterMasterConfig, ipDistribution, ipsWithJobs,
                accountantActor, limiterActor, processingJobDao,
                SourceDocumentReferenceDao, machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao, LOG);

    }

    public static void checkForNewFastLaneJobs(ClusterMasterConfig clusterMasterConfig, Map<String, Integer> ipDistribution,
                                               HashMap<String, Boolean> ipsWithJobs, ActorRef accountantActor,ActorRef limiterActor, ProcessingJobDao processingJobDao,
                                               SourceDocumentReferenceDao SourceDocumentReferenceDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                               Logger LOG) {

        checkForNewJobsByPriority(JobPriority.FASTLANE, clusterMasterConfig, ipDistribution, ipsWithJobs,
                accountantActor, limiterActor, processingJobDao,
                SourceDocumentReferenceDao, machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao, LOG);

    }

    public static void checkForNewJobsByPriority(JobPriority jobPriority, ClusterMasterConfig clusterMasterConfig, Map<String, Integer> ipDistribution,
                                                 HashMap<String, Boolean> ipsWithJobs, ActorRef accountantActor,ActorRef limiterActor, ProcessingJobDao processingJobDao,
                                                 SourceDocumentReferenceDao SourceDocumentReferenceDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                                 final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                                 Logger LOG) {
        final int taskSize = getAllTasks(accountantActor, LOG);

        LOG.debug("{} priority - Starting job loading, tasksize = {}", jobPriority.name(), taskSize);

        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                "{} priority - Checking IPs in database", jobPriority.name());

        List<MachineResourceReference> ips = machineResourceReferenceDao.getAllMachineResourceReferences(new Page(0, 10000));
        for (MachineResourceReference machine : ips) {
            if (!ipDistribution.containsKey(machine.getIp())) {
                ipDistribution.put(machine.getIp(), 0);
            }
        }

        // Set all the limits found in the machine resource reference table (unless null)
        for (final MachineResourceReference reference : ips) {
            if (reference.getMaxConcurrentConnectionsLimit() != null) {
                limiterActor.tell(new ChangeMaxAvailableSlotsRequest(reference.getIp(),reference.getMaxConcurrentConnectionsLimit()),ActorRef.noSender());
            }
        }

        if (taskSize < clusterMasterConfig.getMaxTasksInMemory()) {

            //don't load for IPs that are overloaded
            ArrayList<String> noLoadIPs = getOverLoadedIPList(10000, accountantActor, LOG);
            HashMap<String, Integer> tempDistribution = new HashMap<>(ipDistribution);
            if (noLoadIPs != null) {
                for (String ip : noLoadIPs) {
                    if (tempDistribution.containsKey(ip))
                        tempDistribution.remove(ip);
                }
            }

            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "{} priority - #IPs with tasks: ip temp size {}, ip all size : {}", jobPriority.name(), tempDistribution.size(), ipDistribution.size());

            final Timer.Context loadJobTasksFromDBDuration = MasterMetrics.Master.loadJobTasksFromDBDuration.time();
            final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
            final List<ProcessingJob> all =
                    processingJobDao.getDiffusedJobsWithState(jobPriority, JobState.READY, page, tempDistribution);
            loadJobTasksFromDBDuration.stop();

            // Update the IP with jobs distributed state
            for (ProcessingJob job : all) ipsWithJobs.put(job.getIpAddress(),true);

            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "{} priority - Done with loading {} priority jobs. Creating tasks from them.", jobPriority.name(), all.size());

            final Timer.Context loadJobResourcesFromDBDuration = MasterMetrics.Master.loadJobResourcesFromDBDuration.time();

            final Map<String, SourceDocumentReference> sourceDocumentReferenceIdToDoc = getStringSourceDocumentReferenceMap(SourceDocumentReferenceDao, all);
            final Map<String, SourceDocumentProcessingStatistics> referenceIdTolastJobProcessingStatisticsMap = getSourceDocumentProcessingStatisticsMap(sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceIdToDoc.values());
            loadJobResourcesFromDBDuration.stop();

            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "{} priority -  Done with loading {} resources.", jobPriority.name(), sourceDocumentReferenceIdToDoc.size());

            List<String> processingJobIdsThatAreRunningInHarvester = new ArrayList<>();
            int i = 0;
            for (final ProcessingJob job : all) {
                try {
//
//                    i++;
//                    if (i >= 500) {
                        //LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                            //    "{} priority -  Done with another 500 jobs out of {}", jobPriority.name(), all.size());
//                        i = 0;
//                    }
                    addJob(job, jobPriority.getPriority(), sourceDocumentReferenceIdToDoc, referenceIdTolastJobProcessingStatisticsMap, clusterMasterConfig, processingJobDao, sourceDocumentProcessingStatisticsDao,
                            accountantActor, LOG);

                    processingJobIdsThatAreRunningInHarvester.add(job.getId());
                } catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                            "{} priority -   JobLoaderMasterActor, while loading job: {} -> {}", jobPriority.name(), job.getId(), e.getMessage());
                }
            }

            if (!processingJobIdsThatAreRunningInHarvester.isEmpty()) {
                processingJobDao.modifyStateOfJobsWithIds(JobState.RUNNING, processingJobIdsThatAreRunningInHarvester);
                LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                        "{} priority -   JobLoaderMasterActor, {} new jobs loaded & their state in DB is RUNNING.", jobPriority.name(), processingJobIdsThatAreRunningInHarvester.size());
            }
        }
    }

    private static Map<String, SourceDocumentProcessingStatistics> getSourceDocumentProcessingStatisticsMap(SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao, Collection<SourceDocumentReference> all) {
        final List<String> statsIds = new ArrayList<>();

        for (final SourceDocumentReference reference : all) {
            if (reference.getLastStatsId() != null) statsIds.add(reference.getLastStatsId());
        }

        final List<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatisticsList = sourceDocumentProcessingStatisticsDao.read(statsIds);

        final Map<String, SourceDocumentProcessingStatistics> results = new HashMap<>();

        for (SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics : sourceDocumentProcessingStatisticsList) {
            results.put(sourceDocumentProcessingStatistics.getSourceDocumentReferenceId(), sourceDocumentProcessingStatistics);
        }
        return results;
    }


    private static Map<String, SourceDocumentReference> getStringSourceDocumentReferenceMap(SourceDocumentReferenceDao SourceDocumentReferenceDao, List<ProcessingJob> all) {
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
        final List<SourceDocumentReference> sourceDocumentReferences = SourceDocumentReferenceDao.read(resourceIds);
        final Map<String, SourceDocumentReference> resources = new HashMap<>();
        for (SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
            resources.put(sourceDocumentReference.getId(), sourceDocumentReference);
        }
        return resources;
    }


    private static int getAllTasks(ActorRef accountantActor, Logger LOG) {
        final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new GetNumberOfTasks(), timeout);
        int tasks = 0;
        try {
            tasks = (int) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Exception while getting all tasks", e);
        }

        return tasks;
    }

    private static ArrayList<String> getOverLoadedIPList(int threshold, ActorRef accountantActor, Logger LOG) {
        final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(30, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new GetOverLoadedIPs(threshold), timeout);
        ArrayList<String> ips = null;
        try {
            ips = (ArrayList<String>) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "OverloadedIPs", e);
        }

        return ips;
    }


    /**
     * Adds a job and its tasks to our evidence.
     *
     * @param job the ProcessingJob object
     */
    private static void addJob(final ProcessingJob job, final Integer jobPriority, final Map<String, SourceDocumentReference> resources, final Map<String, SourceDocumentProcessingStatistics> lastJobProcessingStatistics,
                               final ClusterMasterConfig clusterMasterConfig, final ProcessingJobDao processingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final ActorRef accountantActor, Logger LOG) {

        // (Step 1) Generate the tasks.
        final List<ProcessingJobTaskDocumentReference> tasks = job.getTasks();

        final List<RetrieveUrl> generatedTasks = new ArrayList<>();
        for (final ProcessingJobTaskDocumentReference task : tasks) {
            final RetrieveUrl retrieveUrl = generateTask(job, task, resources, lastJobProcessingStatistics, LOG);
            if (retrieveUrl != null) {
                generatedTasks.add(retrieveUrl);
            }
        }

//        if (tasks.size() > 10)
            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Loaded {} tasks for jobID {} on IP {}", tasks.size(), job.getId(), job.getIpAddress());

        final List<String> taskIds = new ArrayList<>();
        for (final RetrieveUrl retrieveUrl : generatedTasks) {
            taskIds.add(retrieveUrl.getId());
        }

        // (Step 2) Send the tasks to accountant


        for (final RetrieveUrl retrieveUrl : generatedTasks) {
            accountantActor.tell(new AddTask(job.getPriority(), retrieveUrl.getId(), new Pair<>(retrieveUrl, TaskState.READY)), ActorRef.noSender());
        }


    }

    /**
     * Loads the task and all needed resources for that task.
     *
     * @param job  the job which contains the task
     * @param task the concrete task to load
     * @return generated task ID
     */
    private static RetrieveUrl generateTask(final ProcessingJob job, final ProcessingJobTaskDocumentReference task,
                                            final Map<String, SourceDocumentReference> resources,
                                            final Map<String, SourceDocumentProcessingStatistics> lastJobProcessingStatistics,
                                            Logger LOG) {
        final String sourceDocId = task.getSourceDocumentReferenceID();

        final SourceDocumentReference sourceDocumentReference = resources.get(sourceDocId);
        if (sourceDocumentReference == null) {
            return null;
        }

        final String ipAddress = job.getIpAddress();
        final Map<String, String> headers = lastJobProcessingStatistics.containsKey(task.getSourceDocumentReferenceID()) ? lastJobProcessingStatistics.get(task.getSourceDocumentReferenceID()).getHttpResponseHeaders() : new HashMap<String, String>();
        final RetrieveUrl retrieveUrl = new RetrieveUrl(sourceDocumentReference.getUrl(), job.getLimits(), task.getTaskType(),
                job.getId(), task.getSourceDocumentReferenceID(),
                headers, task, ipAddress, sourceDocumentReference.getReferenceOwner());

        return retrieveUrl;
    }


}
