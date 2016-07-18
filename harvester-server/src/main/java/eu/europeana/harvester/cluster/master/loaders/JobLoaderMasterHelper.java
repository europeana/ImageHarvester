package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.ActorRef;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.inner.AddTask;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.db.interfaces.MachineResourceReferenceDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.*;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobLoaderMasterHelper  {


    public static Map<String, Integer> getIPDistribution( MachineResourceReferenceDao machineResourceReferenceDao, Logger LOG ) {

        Page pg = new Page(0, 100000);
        List<MachineResourceReference> machines = machineResourceReferenceDao.getAllMachineResourceReferences(pg);
        Map<String, Integer> ipDistribution = new HashMap<>();

        for (MachineResourceReference machine : machines)
            ipDistribution.put(machine.getIp(), 0);

        LOG.debug("IP distribution: ");
//        for (Map.Entry<String, Integer> ip : ipDistribution.entrySet()) {
//            LOG.debug("{}: {}", ip.getKey(), ip.getValue());
//            //machineResourceReferenceDao.createOrModify(new MachineResourceReference(ip.getKey()), WriteConcern.NORMAL);
//        }

        LOG.debug("Nr. of machines: {}", ipDistribution.size());
        LOG.debug("End of IP distribution");
        return ipDistribution;
    }

    /**
     * Adds a job and its tasks to our evidence.
     *
     * @param job the ProcessingJob object
     */
    private static void addJob(final ProcessingJob job, final int jobPriority, final Map<String, SourceDocumentReference> resources,
                               final ClusterMasterConfig clusterMasterConfig, final ProcessingJobDao processingJobDao,
                               final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               final ActorRef accountantActor, Logger LOG) {
        final List<ProcessingJobTaskDocumentReference> tasks = job.getTasks();

        final ProcessingJob newProcessingJob = job.withState(JobState.RUNNING);
        processingJobDao.update(newProcessingJob, clusterMasterConfig.getWriteConcern());

        for (final ProcessingJobTaskDocumentReference task : tasks) {
            final String ID = processTask(job, task, resources, sourceDocumentProcessingStatisticsDao, accountantActor,   LOG);
        }

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
                                           final SourceDocumentReference newDoc,
                                           final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao ) {
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
     * Checks if any job was started but due to an issue of this node it has been abandoned.
     */
    public static void checkForAbandonedJobs(ProcessingJobDao processingJobDao, ClusterMasterConfig clusterMasterConfig,
                                             Logger LOG ) {

        processingJobDao.modifyStateOfJobs(JobState.RUNNING,JobState.READY);

    }

}
