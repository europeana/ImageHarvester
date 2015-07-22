package eu.europeana.harvester.client;

import com.google.code.morphia.Key;
import com.google.code.morphia.Datastore;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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
                new MachineResourceReferenceDaoImpl(datastore),
                new SourceDocumentProcessingStatisticsDaoImpl(datastore),
                new LastSourceDocumentProcessingStatisticsDaoImpl(datastore),
                new SourceDocumentReferenceDaoImpl(datastore),
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore),
                new SourceDocumentReferenceProcessingProfileDaoImpl(datastore),
             harvesterClientConfig);
    }

    public HarvesterClientImpl (ProcessingJobDao processingJobDao, MachineResourceReferenceDao machineResourceReferenceDao,
                                SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao,
                                SourceDocumentReferenceDao SourceDocumentReferenceDao,
                                SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao,
                                SourceDocumentReferenceProcessingProfileDao sourceDocumentReferenceProcessingProfileDao,
                                HarvesterClientConfig harvesterClientConfig) {

        this.processingJobDao = processingJobDao;
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
    public Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModifySourceDocumentReference(Collection<SourceDocumentReference> sourceDocumentReferences) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        if (null == sourceDocumentReferences || sourceDocumentReferences.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        //LOG.debug("Create or modify {} SourceDocumentReferences documents ",sourceDocumentReferences.size());
        final List<MachineResourceReference> machineResourceReferences = new ArrayList<>();

        // Prepare all the machine references
        for (final SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
            machineResourceReferences.add(new MachineResourceReference(cachingUrlResolver.resolveIpOfUrl(sourceDocumentReference.getUrl())));
        }

        // Persist everything.
        machineResourceReferenceDao.createOrModify(machineResourceReferences, harvesterClientConfig.getWriteConcern());
        return sourceDocumentReferenceDao.createOrModify(sourceDocumentReferences, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public Iterable<Key<SourceDocumentReferenceProcessingProfile>> createOrModifyProcessingProfiles (Collection<SourceDocumentReferenceProcessingProfile> profiles) {
        return sourceDocumentReferenceProcessingProfileDao.createOrModify(profiles, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public void createOrModifyProcessingJobTuples (Collection<ProcessingJobTuple> jobTuples) throws
                                                                                             InterruptedException,
                                                                                             MalformedURLException,
                                                                                             TimeoutException,
                                                                                             ExecutionException,
                                                                                             UnknownHostException {
        final Collection <ProcessingJob> processingJobs = new ArrayList<>(jobTuples.size());
        final Collection <SourceDocumentReference> sourceDocumentReferences = new ArrayList<>(jobTuples.size());
        final Collection <SourceDocumentReferenceProcessingProfile> processingProfiles = new ArrayList<>(jobTuples.size());

        for (final ProcessingJobTuple jobTuple: jobTuples) {
            processingJobs.add(jobTuple.getProcessingJob());
            sourceDocumentReferences.add(jobTuple.getSourceDocumentReference());
            processingProfiles.addAll(jobTuple.getSourceDocumentReferenceProcessingProfiles());
        }

        createOrModifySourceDocumentReference(sourceDocumentReferences);
        createOrModify(processingJobs);
        createOrModifyProcessingProfiles(processingProfiles);
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
    public List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public ProcessingJob retrieveProcessingJob(String jobId) {
        return processingJobDao.read(jobId);
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
                                                                     harvesterClientConfig.getWriteConcern()
                                                                    );
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

        if (processingJobs.isEmpty()) return  processingJobs;

        final List<String> sourceDocumentReferenceIds = new ArrayList<>(processingJobs.size());

        for (final SourceDocumentReference documentReference: sourceDocumentReferenceDao.deactivateDocuments(owner, harvesterClientConfig.getWriteConcern())) {
           sourceDocumentReferenceIds.add(documentReference.getId());
        }

        sourceDocumentProcessingStatisticsDao.deactivateDocuments(sourceDocumentReferenceIds, harvesterClientConfig.getWriteConcern()).clear();
        lastSourceDocumentProcessingStatisticsDao.deactivateDocuments(sourceDocumentReferenceIds, harvesterClientConfig.getWriteConcern()).clear();
        sourceDocumentReferenceProcessingProfileDao.deactivateDocuments(owner, harvesterClientConfig.getWriteConcern()).clear();

        return processingJobs;
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

}
