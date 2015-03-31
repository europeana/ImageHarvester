package eu.europeana.harvester.client;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * The meeting point between the client and the application.
 */
public class HarvesterClientImpl implements HarvesterClient {

        private static final Logger LOG = LogManager.getLogger(HarvesterClientImpl.class.getName());

    /**
     * DAO for CRUD with processing_job collection
     */
    private final ProcessingJobDao processingJobDao;

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
     * SourceDocumentReferenceMetaInfo DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    /**
     * DAO for CRUD with link_check_limit collection
     */
    private final LinkCheckLimitsDao linkCheckLimitsDao;

    /**
     * An object which contains different special configurations for Harvester Client.
     */
    private final HarvesterClientConfig harvesterClientConfig;

    public HarvesterClientImpl(final MorphiaDataStore datastore, final HarvesterClientConfig harvesterClientConfig) {
        this(new ProcessingJobDaoImpl(datastore.getDatastore()),
                new MachineResourceReferenceDaoImpl(datastore.getDatastore()),
                new SourceDocumentProcessingStatisticsDaoImpl(datastore.getDatastore()),
                new SourceDocumentReferenceDaoImpl(datastore.getDatastore()),
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore.getDatastore()),
                new LinkCheckLimitsDaoImpl(datastore.getDatastore()), harvesterClientConfig);
    }

    public HarvesterClientImpl(ProcessingJobDao processingJobDao, MachineResourceReferenceDao machineResourceReferenceDao,
                               SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao, LinkCheckLimitsDao linkCheckLimitsDao, HarvesterClientConfig harvesterClientConfig) {

        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.linkCheckLimitsDao = linkCheckLimitsDao;
        this.harvesterClientConfig = harvesterClientConfig;
    }

    @Override
    public void createOrModifyLinkCheckLimits(LinkCheckLimits linkCheckLimits) {
        LOG.info("Create or modify link check limits");

        linkCheckLimitsDao.createOrModify(linkCheckLimits, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public void createOrModifyProcessingLimits(MachineResourceReference machineResourceReference) {
        LOG.info("Create or modify processing limits");

        machineResourceReferenceDao.createOrModify(machineResourceReference, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public void createOrModifySourceDocumentReference(List<SourceDocumentReference> sourceDocumentReferences) {
        LOG.info("Create or modify SourceDocumentReferences");

        for(final SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
            sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, harvesterClientConfig.getWriteConcern());
        }
    }

    @Override
    public ProcessingJob createProcessingJob(ProcessingJob processingJob) {
        LOG.info("Create processing job");

        processingJobDao.create(processingJob, harvesterClientConfig.getWriteConcern());
        return processingJob;
    }

    @Override
    public ProcessingJob createProcessingJobForCollection(String collectionId, DocumentReferenceTaskType type) {
        throw new NotImplementedException();
    }

    @Override
    public ProcessingJob createProcessingJobForRecord(String recordId, DocumentReferenceTaskType type) {
        throw new NotImplementedException();
    }

    @Override
    public ProcessingJob stopJob(String jobId) {
        LOG.info("Stopping job with id: {}", jobId);
        final ProcessingJob processingJob = processingJobDao.read(jobId);
        final JobState currentState = processingJob.getState();
        if((JobState.RUNNING).equals(currentState) ||
                (JobState.RESUME).equals(currentState) ||
                (JobState.READY).equals(currentState)) {
            final ProcessingJob newProcessingJob = processingJob.withState(JobState.PAUSE);
            processingJobDao.update(newProcessingJob, harvesterClientConfig.getWriteConcern());

            return newProcessingJob;
        }

        return processingJob;
    }

    @Override
    public ProcessingJob startJob(String jobId) {
        LOG.info("Starting job with id: {}", jobId);
        final ProcessingJob processingJob = processingJobDao.read(jobId);
        final ProcessingJob newProcessingJob = processingJob.withState(JobState.RESUME);
        processingJobDao.update(newProcessingJob, harvesterClientConfig.getWriteConcern());

        return newProcessingJob;
    }

    @Override
    public List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state) {
        throw new NotImplementedException();
    }

    @Override
    public ProcessingJobStats statsOfJob(String jobId) {
        LOG.info("Retrieving job stats.");
        final ProcessingJob processingJob = processingJobDao.read(jobId);

        final Map<ProcessingState, Set<String>> recordIdsByState = new HashMap<ProcessingState, Set<String>>();
        final Map<ProcessingState, Set<String>> sourceDocumentReferenceIdsByState =
                new HashMap<ProcessingState, Set<String>>();

        for(final ProcessingJobTaskDocumentReference task : processingJob.getTasks()) {
            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(
                            task.getSourceDocumentReferenceID(), jobId);

            if(sourceDocumentProcessingStatistics != null) {
                final ProcessingState processingState = sourceDocumentProcessingStatistics.getState();
                Set<String> recordIds = recordIdsByState.get(processingState);
                if(recordIds == null) {
                    recordIds = new HashSet<String>();
                }
                recordIds.add(sourceDocumentProcessingStatistics.getReferenceOwner().getRecordId());

                Set<String> sourceDocIds = sourceDocumentReferenceIdsByState.get(processingState);
                if(sourceDocIds == null) {
                    sourceDocIds = new HashSet<String>();
                }
                sourceDocIds.add(sourceDocumentProcessingStatistics.getSourceDocumentReferenceId());

                recordIdsByState.put(processingState, recordIds);
                sourceDocumentReferenceIdsByState.put(processingState, sourceDocIds);
            }
        }

        return new ProcessingJobStats(recordIdsByState, sourceDocumentReferenceIdsByState);
    }

    @Override
    public SourceDocumentReferenceMetaInfo retrieveMetaInfoByUrl(String url) {
        final HashFunction hf = Hashing.md5();
        final HashCode hc = hf.newHasher()
                .putString(url, Charsets.UTF_8)
                .hash();
        final String id = hc.toString();

        return sourceDocumentReferenceMetaInfoDao.read(id);
    }

    @Override
    public void setActive(String recordID, Boolean active) {
        final List<SourceDocumentReference> sourceDocumentReferenceList =
                sourceDocumentReferenceDao.findByRecordID(recordID);
        final List<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatisticsList =
                sourceDocumentProcessingStatisticsDao.findByRecordID(recordID);

        final List<SourceDocumentReference> newSourceDocumentReferenceList = new ArrayList<>();

        for(final SourceDocumentReference sourceDocumentReference : sourceDocumentReferenceList) {
            final SourceDocumentReference newSourceDocumentReference = sourceDocumentReference.withActive(active);
            newSourceDocumentReferenceList.add(newSourceDocumentReference);
        }

        for(final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics : sourceDocumentProcessingStatisticsList) {
            final SourceDocumentProcessingStatistics newSourceDocumentProcessingStatistics =
                    sourceDocumentProcessingStatistics.withActive(active);
            sourceDocumentProcessingStatisticsDao.createOrUpdate(newSourceDocumentProcessingStatistics,
                    harvesterClientConfig.getWriteConcern());
        }

        createOrModifySourceDocumentReference(newSourceDocumentReferenceList);
    }

    @Override
    public boolean update(SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo){
        return sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo, WriteConcern.NORMAL);
    }

    @Override
    public void updateSourceDocumentProcesssingStatisticsForUrl(String url){

        final HashFunction hf = Hashing.md5();
        final HashCode hc = hf.newHasher()
                .putString(url, Charsets.UTF_8)
                .hash();
        final String id = hc.toString();

        SourceDocumentProcessingStatistics s = this.sourceDocumentProcessingStatisticsDao.read(id);
        this.sourceDocumentProcessingStatisticsDao.update(s.withActive(true),WriteConcern.NORMAL);



    }


}
