package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.cluster.master.MasterMetrics;
import eu.europeana.harvester.db.interfaces.MachineResourceReferenceDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JobLoaderExecutorHelper  {

    /**
     * Checks if there were added any new jobs in the db
     */
    public static void checkForNewJobs(ClusterMasterConfig clusterMasterConfig, Map<String, Integer> ipDistribution,
                                       HashMap<String, Boolean> ipsWithJobs , ActorRef accountantActor,ProcessingJobDao processingJobDao,
                                       SourceDocumentReferenceDao sourceDocumentReferenceDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                       final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                       Logger LOG) {
        final int taskSize = getAllTasks(accountantActor, LOG);

        LOG.info("Starting normal job loading, tasksize = "+taskSize);

        if (taskSize < clusterMasterConfig.getMaxTasksInMemory()) {
            //don't load for IPs that are overloaded
            ArrayList<String> noLoadIPs = getOverLoadedIPList(1000, accountantActor, LOG);
            HashMap<String, Integer> tempDistribution = new HashMap<>(ipDistribution);
            if (noLoadIPs != null) {
                for (String ip : noLoadIPs) {
                    if (tempDistribution.containsKey(ip))
                        tempDistribution.remove(ip);
                }
            }


            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "#IPs with tasks: temp "+tempDistribution.size()+", all: "+ipDistribution.size());

            final Timer.Context loadJobTasksFromDBDuration = MasterMetrics.Master.loadJobTasksFromDBDuration.time();
            final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
            final List<ProcessingJob> all =
                    processingJobDao.getDiffusedJobsWithState(JobPriority.NORMAL, JobState.READY, page, tempDistribution, ipsWithJobs);
            loadJobTasksFromDBDuration.stop();

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Done with loading normal priority jobs. Creating tasks from "+all.size()+" jobs.");

            final Timer.Context loadJobResourcesFromDBDuration = MasterMetrics.Master.loadJobResourcesFromDBDuration.time();

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
            loadJobResourcesFromDBDuration.stop();

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Done with loading {} resources.",resources.size());

            int i = 0;
            for (final ProcessingJob job : all) {
                try {

                    i++;
                    if (i >= 500) {
                        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                                "Done with another 500 jobs out of {}", all.size());
                        i = 0;
                    }
                    addJob(job, JobPriority.NORMAL.getPriority(), resources, clusterMasterConfig, processingJobDao, sourceDocumentProcessingStatisticsDao,
                            accountantActor, LOG);

                } catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                            "JobLoaderMasterActor, while loading job: {} -> {}", job.getId(), e.getMessage());
                }
            }
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Checking IPs with no jobs in database");

//            ArrayList<String> noJobsIPs = new ArrayList<>();
//            List<MachineResourceReference> ips = machineResourceReferenceDao.getAllMachineResourceReferences(new Page(0, 10000));
//
//            for (Map.Entry<String, Boolean> entry : ipsWithJobs.entrySet()) {
//                if (!entry.getValue()) {
//                    noJobsIPs.add(entry.getKey());
//                    ipDistribution.remove(entry.getKey());
//                    LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
//                            "Found IP with no loaded tasks from DB: {}, removing it from IP distribution", entry.getKey());
//
//                    for (MachineResourceReference machine : ips) {
//                        if (machine.getIp() == entry.getKey()) {
//                            machineResourceReferenceDao.delete(machine.getId());
//                            ips.remove(machine);
//                        }
//                    }
//
//                }
//            }
//
//
//            for (MachineResourceReference machine : ips) {
//                if (!ipDistribution.containsKey(machine.getIp())) {
//                    ipDistribution.put(machine.getIp(), 0);
//                }
//            }
//
//            if (noJobsIPs.size() > 0) {
//                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
//                        "Found {} IPs with no jobs loaded from the database, removing them if no jobs in progress", noJobsIPs.size());
//
//                accountantActor.tell(new CleanIPs(noJobsIPs), ActorRef.noSender());
//            }

        }
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
    private static void addJob(final ProcessingJob job, final Integer jobPriority, final Map<String, SourceDocumentReference> resources,
                               final ClusterMasterConfig clusterMasterConfig, final ProcessingJobDao processingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final ActorRef accountantActor, Logger LOG) {
        final List<ProcessingJobTaskDocumentReference> tasks = job.getTasks();

        final ProcessingJob newProcessingJob = job.withState(JobState.RUNNING);
        processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

        final List<String> taskIDs = new ArrayList<>();
        for (final ProcessingJobTaskDocumentReference task : tasks) {
            final String ID = processTask(job, task, resources, sourceDocumentProcessingStatisticsDao, accountantActor,   LOG);
            if (ID != null) {
                taskIDs.add(ID);
            }
        }

        if (tasks.size() > 10)
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Loaded {} tasks for jobID {} on IP {}", tasks.size(), job.getId(), job.getIpAddress());

        accountantActor.tell(new AddTasksToJob(job.getId(), jobPriority, taskIDs), ActorRef.noSender());
    }

    /**
     * Loads the task and all needed resources for that task.
     *
     * @param job  the job which contains the task
     * @param task the concrete task to load
     * @return generated task ID
     */
    private static String processTask(final ProcessingJob job, final ProcessingJobTaskDocumentReference task,
                               final Map<String, SourceDocumentReference> resources,
                               SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               ActorRef accountantActor, Logger LOG) {
        final String sourceDocId = task.getSourceDocumentReferenceID();

        final SourceDocumentReference sourceDocumentReference = resources.get(sourceDocId);
        if (sourceDocumentReference == null) {
            return null;
        }

        final String ipAddress = job.getIpAddress();

        final RetrieveUrl retrieveUrl = new RetrieveUrl(sourceDocumentReference.getUrl(), job.getLimits(), task.getTaskType(),
                job.getId(), task.getSourceDocumentReferenceID(),
                getHeaders(task.getTaskType(), sourceDocumentReference, sourceDocumentProcessingStatisticsDao ), task, ipAddress,sourceDocumentReference.getReferenceOwner());

        accountantActor.tell(new AddTask(job.getPriority(), retrieveUrl.getId(), new Pair<>(retrieveUrl, TaskState.READY)), ActorRef.noSender());

        return retrieveUrl.getId();
    }

    /**
     * Returns the headers of a source document if we already retrieved that at least once.
     *
     * @param documentReferenceTaskType task type
     * @param newDoc                    source document object
     * @return list of headers
     */
    private static Map<String, String> getHeaders(final DocumentReferenceTaskType documentReferenceTaskType,
                                           final SourceDocumentReference newDoc, final SourceDocumentProcessingStatisticsDao
                                            sourceDocumentProcessingStatisticsDao ) {
        Map<String, String> headers = null;

        if (documentReferenceTaskType == null) {
            return null;
        }

        if ((DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD).equals(documentReferenceTaskType)) {
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
     * Checks if there were added any new fast lane jobs in the db
     */
    public static void checkForNewFastLaneJobs(ClusterMasterConfig clusterMasterConfig, Map<String, Integer> ipDistribution,
                                       HashMap<String, Boolean> ipsWithJobs , ActorRef accountantActor,ProcessingJobDao processingJobDao,
                                       SourceDocumentReferenceDao sourceDocumentReferenceDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                       final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                       Logger LOG) {



            final Timer.Context loadJobTasksFromDBDuration = MasterMetrics.Master.loadFastLaneJobTasksFromDBDuration.time();
            final Page page = new Page(0, clusterMasterConfig.getJobsPerIP());
            final List<ProcessingJob> all =
                    processingJobDao.getDiffusedJobsWithState(JobPriority.FASTLANE, JobState.READY, page, ipDistribution, ipsWithJobs);
            loadJobTasksFromDBDuration.stop();

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Done with loading fastlane jobs. Creating tasks from "+all.size()+" jobs.");

            final Timer.Context loadJobResourcesFromDBDuration = MasterMetrics.Master.loadFastLaneJobResourcesFromDBDuration.time();

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
            loadJobResourcesFromDBDuration.stop();

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                    "Done with loading {} resources.",resources.size());

            int i = 0;
            for (final ProcessingJob job : all) {
                try {

                    i++;
                    if (i >= 500) {
                        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                                "Done with another 500 jobs out of {}", all.size());
                        i = 0;
                    }
                    addJob(job, JobPriority.FASTLANE.getPriority(), resources, clusterMasterConfig, processingJobDao, sourceDocumentProcessingStatisticsDao,
                            accountantActor, LOG);

                } catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                            "JobLoaderMasterActor, while loading job: {} -> {}", job.getId(), e.getMessage());
                }
            }


    }



}
