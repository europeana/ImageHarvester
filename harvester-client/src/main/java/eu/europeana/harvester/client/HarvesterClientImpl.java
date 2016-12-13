package eu.europeana.harvester.client;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.common.collect.Lists;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.domain.report.SubTaskState;
import eu.europeana.harvester.domain.report.SubTaskType;
import eu.europeana.harvester.util.CachingUrlResolver;
import eu.europeana.harvester.util.pagedElements.PagedElements;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.Interval;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * The meeting point between the client and the application.
 */
public class HarvesterClientImpl implements HarvesterClient {

    private static final int MAX_BATCH_SIZE_PROCESSING_TUPLES = 10*1000;

    private static final Logger LOG = LogManager.getLogger(HarvesterClientImpl.class.getName());

    /**
     * DAO for CRUD with processing_job collection
     */
    private final ProcessingJobDao processingJobDao;


    /**
     * DAO for CRUD with processing_job collection
     */
    private final HistoricalProcessingJobDao historicalProcessingJobDao;

    /**
     * DAO for CRUD with machine_resource_reference collection
     */
    private final MachineResourceReferenceDao machineResourceReferenceDao;

    /**
     * DAO for CRUD with source_document_processing_stats collection
     */
    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    /**
     * DAO for CRUD with source_document_reference collection
     */
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    /**
     * SourceDocumentReferenceMetaInfo DAO object which lets us to read and
     * store data to and from the database.
     */
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    private final SourceDocumentReferenceProcessingProfileDao sourceDocumentReferenceProcessingProfileDao;

    /**
     * An object which contains different special configurations for Harvester
     * Client.
     */
    private final HarvesterClientConfig harvesterClientConfig;

    private final CachingUrlResolver cachingUrlResolver;
    private final LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao;

    public HarvesterClientImpl(final Datastore datastore, final HarvesterClientConfig harvesterClientConfig) {
        this(new ProcessingJobDaoImpl(datastore),
                new HistoricalProcessingJobDaoImpl(datastore),
                new MachineResourceReferenceDaoImpl(datastore),
                new SourceDocumentProcessingStatisticsDaoImpl(datastore),
                new LastSourceDocumentProcessingStatisticsDaoImpl(datastore),
                new SourceDocumentReferenceDaoImpl(datastore),
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore),
                new SourceDocumentReferenceProcessingProfileDaoImpl(datastore),
             harvesterClientConfig);
    }

    public HarvesterClientImpl (ProcessingJobDao processingJobDao,HistoricalProcessingJobDao historicalProcessingJobDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao,
                                SourceDocumentReferenceDao SourceDocumentReferenceDao,
                                SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                                SourceDocumentReferenceProcessingProfileDao sourceDocumentReferenceProcessingProfileDao,
                                HarvesterClientConfig harvesterClientConfig) {

        this.processingJobDao = processingJobDao;
        this.historicalProcessingJobDao = historicalProcessingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.lastSourceDocumentProcessingStatisticsDao = lastSourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = SourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.sourceDocumentReferenceProcessingProfileDao = sourceDocumentReferenceProcessingProfileDao;
        this.harvesterClientConfig = harvesterClientConfig;
        this.cachingUrlResolver = new CachingUrlResolver();
    }

    @Override
    public Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModifySourceDocumentReference(Collection<SourceDocumentReference> newSourceDocumentReferences) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        if (null == newSourceDocumentReferences || newSourceDocumentReferences.isEmpty()) {
            return Collections.emptyList();
        }

        //LOG.debug("Create or modify {} SourceDocumentReferences documents ",sourceDocumentReferences.size());
        final List<MachineResourceReference> machineResourceReferences = new ArrayList<>();

        // Prepare all the machine references
        for (final SourceDocumentReference sourceDocumentReference : newSourceDocumentReferences) {
            machineResourceReferences.add(new MachineResourceReference(cachingUrlResolver.resolveIpOfUrlAndReturnLoopbackOnFail(sourceDocumentReference.getUrl())));
        }

        // Retrieve all the existing source document references as these might need to be updated
        List<String> sourceDocumentReferenceIds = new ArrayList<String>();
        for (final SourceDocumentReference sourceDocumentReference : newSourceDocumentReferences) {
            sourceDocumentReferenceIds.add(sourceDocumentReference.getId());
        }

        Map<String,SourceDocumentReference> sourceDocumentReferenceIdsToDoc = new HashMap<String,SourceDocumentReference>();

        for (final SourceDocumentReference existingSourceDocumentReference :  sourceDocumentReferenceDao.read(sourceDocumentReferenceIds)) {
            sourceDocumentReferenceIdsToDoc.put(existingSourceDocumentReference.getId(),existingSourceDocumentReference);
        }

        List<SourceDocumentReference> toBePersistedSourceDocumentReferences = new ArrayList<SourceDocumentReference>();
        for (final SourceDocumentReference newSourceDocumentReference : newSourceDocumentReferences) {
            if (sourceDocumentReferenceIdsToDoc.containsKey(newSourceDocumentReference.getId())){
                // We need to merge them
                toBePersistedSourceDocumentReferences.add(newSourceDocumentReference.withLastStatsId(sourceDocumentReferenceIdsToDoc.get(newSourceDocumentReference.getId()).getLastStatsId()));
            } else {
                toBePersistedSourceDocumentReferences.add(newSourceDocumentReference);
            }
        }

        // Persist everything.
        machineResourceReferenceDao.createOrModify(machineResourceReferences, harvesterClientConfig.getWriteConcern());
        return sourceDocumentReferenceDao.createOrModify(toBePersistedSourceDocumentReferences, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public Iterable<Key<SourceDocumentReferenceProcessingProfile>> createOrModifyProcessingProfiles (Collection<SourceDocumentReferenceProcessingProfile> profiles) {
        return sourceDocumentReferenceProcessingProfileDao.createOrModify(profiles, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public void createOrModifyProcessingJobTuples (List<ProcessingJobTuple> jobTuples) throws
                                                                                             InterruptedException,
                                                                                             MalformedURLException,
                                                                                             TimeoutException,
                                                                                             ExecutionException,
                                                                                             UnknownHostException {

        for (List<ProcessingJobTuple> oneBatch : Lists.partition(jobTuples, MAX_BATCH_SIZE_PROCESSING_TUPLES)) {

            final Collection<ProcessingJob> processingJobs = new ArrayList<>();
            final Collection<SourceDocumentReference> sourceDocumentReferences = new ArrayList<>();
//            final Collection<SourceDocumentReferenceProcessingProfile> processingProfiles = new ArrayList<>();

            for (final ProcessingJobTuple jobTuple : oneBatch) {
                processingJobs.add(jobTuple.getProcessingJob());
                sourceDocumentReferences.add(jobTuple.getSourceDocumentReference());
                // processingProfiles.addAll(jobTuple.getSourceDocumentReferenceProcessingProfiles());
            }

            createOrModifySourceDocumentReference(sourceDocumentReferences);
            createOrModify(processingJobs);
            // createOrModifyProcessingProfiles(processingProfiles);
        }
    }
    @Override
    public com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJob) {
        return processingJobDao.createOrModify(processingJob, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(Collection<ProcessingJob> processingJobs) {
        return processingJobDao.createOrModify(processingJobs, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public ProcessingJob stopJob(String jobId) {
        //LOG.debug("Stopping job with id: {}", jobId);
        final ProcessingJob processingJob = processingJobDao.read(jobId);
        final JobState currentState = processingJob.getState();
        if ((JobState.RUNNING).equals(currentState)
                || (JobState.RESUME).equals(currentState)
                || (JobState.READY).equals(currentState)) {
            final ProcessingJob newProcessingJob = processingJob.withState(JobState.PAUSE);
            processingJobDao.update(newProcessingJob, harvesterClientConfig.getWriteConcern());

            return newProcessingJob;
        }
        return processingJob;
    }

    @Override
    public ProcessingJob startJob(String jobId) {
        //LOG.debug("Starting job with id: {}", jobId);
        final ProcessingJob processingJob = processingJobDao.read(jobId);
        final ProcessingJob newProcessingJob = processingJob.withState(JobState.RESUME);
        processingJobDao.update(newProcessingJob, harvesterClientConfig.getWriteConcern());
        return newProcessingJob;
    }

    @Override
    public PagedElements<ProcessingJob> findJobsByCollectionAndState(final List<String> collectionIds,
                                                                     final List<JobState> states,
                                                                     final Page pageConfig) throws Exception {
        return processingJobDao.findJobsByCollectionIdAndState(
                new HashSet<>(collectionIds),
                new HashSet<>(states),
                pageConfig
        );
    }

    @Override
    public ProcessingJob retrieveProcessingJob(String jobId) {
        return processingJobDao.read(jobId);
    }


    @Override
    public HistoricalProcessingJob retrieveHistoricalProcessingJob(String jobId) {
        return historicalProcessingJobDao.read(jobId);
    }

    @Override
    public SourceDocumentReference retrieveSourceDocumentReferenceByUrl(String url,String recordId) {
        return sourceDocumentReferenceDao.read(SourceDocumentReference.idFromUrl(url,recordId));
    }

    @Override
    public SourceDocumentReference retrieveSourceDocumentReferenceById(String id) {
        return sourceDocumentReferenceDao.read(id);
    }

    @Override
    public List<SourceDocumentReference> retrieveSourceDocumentReferencesByIds(List<String> id) {
        return sourceDocumentReferenceDao.read(id);
    }

    @Override
    public SourceDocumentReferenceMetaInfo retrieveMetaInfoByUrl(String url) {
        return sourceDocumentReferenceMetaInfoDao.read(SourceDocumentReferenceMetaInfo.idFromUrl(url));
    }

    @Override
    public void setActive(String recordID, Boolean active) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        final List<SourceDocumentReference> sourceDocumentReferenceList
                = sourceDocumentReferenceDao.findByRecordID(recordID);
        final List<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatisticsList
                = sourceDocumentProcessingStatisticsDao.findByRecordID(recordID);

        final List<LastSourceDocumentProcessingStatistics> lastSourceDocumentProcessingStatisticsList
                = lastSourceDocumentProcessingStatisticsDao.findByRecordID(recordID);

        final List<SourceDocumentReference> newSourceDocumentReferenceList = new ArrayList<>();

        for (final SourceDocumentReference sourceDocumentReference : sourceDocumentReferenceList) {
            final SourceDocumentReference newSourceDocumentReference = sourceDocumentReference.withActive(active);
            newSourceDocumentReferenceList.add(newSourceDocumentReference);
        }

        for (final LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatistics: lastSourceDocumentProcessingStatisticsList) {
            lastSourceDocumentProcessingStatisticsDao.createOrModify(lastSourceDocumentProcessingStatistics.withActive(true),
                                                                     harvesterClientConfig.getWriteConcern());
        }

        for (final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics : sourceDocumentProcessingStatisticsList) {
            final SourceDocumentProcessingStatistics newSourceDocumentProcessingStatistics
                    = sourceDocumentProcessingStatistics.withActive(active);
            sourceDocumentProcessingStatisticsDao.createOrModify(newSourceDocumentProcessingStatistics,
                    harvesterClientConfig.getWriteConcern());
        }

        createOrModifySourceDocumentReference(newSourceDocumentReferenceList);
    }

    @Override
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo) {
        return sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public List<ProcessingJob> deactivateJobs (final ReferenceOwner owner) {
        final List<ProcessingJob> processingJobs = processingJobDao.deactivateJobs(owner, harvesterClientConfig.getWriteConcern());

        if (processingJobs.isEmpty()) return processingJobs;

        final List<String> sourceDocumentReferenceIds = new ArrayList<>(processingJobs.size());

        //TODO paginate these methods
        List<SourceDocumentReference> deactivateDocuments = sourceDocumentReferenceDao.deactivateDocuments(owner, harvesterClientConfig.getWriteConcern());
		for (final SourceDocumentReference documentReference: deactivateDocuments) {
           sourceDocumentReferenceIds.add(documentReference.getId());
        }

        sourceDocumentProcessingStatisticsDao.deactivateDocuments(sourceDocumentReferenceIds, harvesterClientConfig.getWriteConcern()).clear();
        lastSourceDocumentProcessingStatisticsDao.deactivateDocuments(sourceDocumentReferenceIds, harvesterClientConfig.getWriteConcern()).clear();
        sourceDocumentReferenceProcessingProfileDao.deactivateDocuments(owner, harvesterClientConfig.getWriteConcern()).clear();
        return processingJobs;
    }

    @Override
    public Map<JobState, Long> countProcessingJobsByState(final String executionId) {
        return lastSourceDocumentProcessingStatisticsDao.countProcessingJobsByState(executionId);
    }

    @Override
    public Interval getDateIntervalForProcessing(final String executionId) {
        return lastSourceDocumentProcessingStatisticsDao.getDateIntervalForProcessing(executionId);
    }

    @Override
    public Map<SubTaskState,Long> countSubTaskStatesByUrlSourceType(final String collectionId,final URLSourceType urlSourceType,final SubTaskType subtaskType) {
       return lastSourceDocumentProcessingStatisticsDao.countSubTaskStatesByUrlSourceType(collectionId, urlSourceType, subtaskType);
    }
    
	@Override
	public Long countSubtaskStatesByUrlSourceType(String collectionId, URLSourceType urlSourceType, SubTaskType subTaskType, SubTaskState subTaskState) {
		return lastSourceDocumentProcessingStatisticsDao.countSubTaskStatesByUrlSourceType(collectionId, urlSourceType, subTaskType, subTaskState);
	}

    @Override
    public Map<ProcessingState,Long> countJobStatesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType, final DocumentReferenceTaskType documentReferenceTaskType) {
        return lastSourceDocumentProcessingStatisticsDao.countJobStatesByUrlSourceType(collectionId, urlSourceType, documentReferenceTaskType);
    }
    
	@Override
	public Long countAllTaskTypesByUrlSourceType(String collectionId, URLSourceType urlSourceType) {
		return lastSourceDocumentProcessingStatisticsDao.countAllTaskTypesByUrlSourceType(collectionId, urlSourceType);
	}

	@Override
	public Long countSuccessfulTaskTypesByUrlSourceType(String collectionId, URLSourceType urlSourceType) {
		return lastSourceDocumentProcessingStatisticsDao.countSuccessfulTaskTypesByUrlSourceType(collectionId, urlSourceType);
	}

    @Override
    public List<LastSourceDocumentProcessingStatistics> findLastSourceDocumentProcessingStatistics(final String collectionId,final String executionId,final List<ProcessingState> processingStates) {
        return lastSourceDocumentProcessingStatisticsDao.findLastSourceDocumentProcessingStatistics(collectionId,executionId,processingStates);
    }

    @Override
    public void updateSourceDocumentProcesssingStatistics(final String sourceDocumentReferenceId, final String processingJobId) {
        SourceDocumentProcessingStatistics s = this.sourceDocumentProcessingStatisticsDao.read(SourceDocumentProcessingStatistics.idOf(sourceDocumentReferenceId, processingJobId));
        if (s != null) {
            this.sourceDocumentProcessingStatisticsDao.update(s.withActive(true), harvesterClientConfig.getWriteConcern());
        }

    }

    @Override
    public SourceDocumentProcessingStatistics readSourceDocumentProcesssingStatistics(final String sourceDocumentReferenceId, final String processingJobId) {
        return this.sourceDocumentProcessingStatisticsDao.read(SourceDocumentProcessingStatistics.idOf(sourceDocumentReferenceId, processingJobId));
    }
    
    @Override
    public Map<String,JobStatistics> findJobsByCollectionId(String collectionId){
    	return this.processingJobDao.findJobsByCollectionId(collectionId);
    }
}
