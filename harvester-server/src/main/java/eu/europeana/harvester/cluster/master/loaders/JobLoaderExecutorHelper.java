package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.ActorRef;
import akka.event.LoggingAdapter;
import akka.pattern.Patterns;
import akka.util.Timeout;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;
import eu.europeana.harvester.domain.*;
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
                                       LoggingAdapter LOG) {
        final int taskSize = getAllTasks(accountantActor, LOG);

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
                    addJob(job, resources, clusterMasterConfig, processingJobDao, sourceDocumentProcessingStatisticsDao,
                            accountantActor, LOG);

                } catch (Exception e) {
                    LOG.error("JobLoaderMasterActor, while loading job: {} -> {}", job.getId(), e.getMessage());
                }
            }

            LOG.info("Checking IPs with no jobs in database");

            ArrayList<String> noJobsIPs = new ArrayList<>();
            List<MachineResourceReference> ips = machineResourceReferenceDao.getAllMachineResourceReferences(new Page(0, 10000));

            for (Map.Entry<String, Boolean> entry : ipsWithJobs.entrySet()) {
                if (!entry.getValue()) {
                    noJobsIPs.add(entry.getKey());
                    ipDistribution.remove(entry.getKey());
                    LOG.info("Found IP with no loaded tasks from DB: {}, removing it from IP distribution", entry.getKey());
                    for (MachineResourceReference machine : ips) {
                        if (machine.getIp() == entry.getKey()) {
                            machineResourceReferenceDao.delete(machine.getId());
                            ips.remove(machine);
                        }
                    }

                }
            }


            for (MachineResourceReference machine : ips) {
                if (!ipDistribution.containsKey(machine.getIp())) {
                    ipDistribution.put(machine.getIp(), 0);
                }
            }

            if (noJobsIPs.size() > 0) {
                LOG.info("Found {} IPs with no jobs loaded from the database, removing them if no jobs in progress", noJobsIPs.size());
                accountantActor.tell(new CleanIPs(noJobsIPs), ActorRef.noSender());
            }

        }
    }


    private static int getAllTasks(ActorRef accountantActor, LoggingAdapter LOG) {
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

    private static ArrayList<String> getOverLoadedIPList(int threshold, ActorRef accountantActor, LoggingAdapter LOG) {
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
     * Adds a job and its tasks to our evidence.
     *
     * @param job the ProcessingJob object
     */
    private static void addJob(final ProcessingJob job, final Map<String, SourceDocumentReference> resources,
                               final ClusterMasterConfig clusterMasterConfig, final ProcessingJobDao processingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final ActorRef accountantActor, LoggingAdapter LOG) {
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
            LOG.info("Loaded {} tasks for jobID {} on IP {}", tasks.size(), job.getId(), job.getIpAddress());

        accountantActor.tell(new AddTasksToJob(job.getId(), taskIDs), ActorRef.noSender());
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
                               ActorRef accountantActor, LoggingAdapter LOG) {
        final String sourceDocId = task.getSourceDocumentReferenceID();

        final SourceDocumentReference sourceDocumentReference = resources.get(sourceDocId);
        if (sourceDocumentReference == null) {
            return null;
        }

        final String ipAddress = job.getIpAddress();

        final RetrieveUrl retrieveUrl = new RetrieveUrl(sourceDocumentReference.getUrl(), job.getLimits(), task.getTaskType(),
                job.getId(), task.getSourceDocumentReferenceID(),
                getHeaders(task.getTaskType(), sourceDocumentReference, sourceDocumentProcessingStatisticsDao ), task, ipAddress,sourceDocumentReference.getReferenceOwner());

        List<String> tasksFromIP = null;
        final Timeout timeout = new Timeout(scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
        final Future<Object> future = Patterns.ask(accountantActor, new GetTasksFromIP(ipAddress), timeout);
        try {
            tasksFromIP = (List<String>) Await.result(future, timeout.duration());
        } catch (Exception e) {
            LOG.error("Error in processTask->GetTasksFromIP: {}", e);
        }

        if (tasksFromIP == null) {
            tasksFromIP = new ArrayList<>();
        } else {
            if (tasksFromIP.contains(retrieveUrl.getId())) {
                return null;
            }
        }
        tasksFromIP.add(retrieveUrl.getId());


        accountantActor.tell(new AddTasksToIP(ipAddress, tasksFromIP), ActorRef.noSender());
        accountantActor.tell(new AddTask(retrieveUrl.getId(), new Pair<>(retrieveUrl, TaskState.READY)), ActorRef.noSender());

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


}
