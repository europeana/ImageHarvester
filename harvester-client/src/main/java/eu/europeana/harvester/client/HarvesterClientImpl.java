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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;
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

    /**
     * An object which contains different special configurations for Harvester
     * Client.
     */
    private final HarvesterClientConfig harvesterClientConfig;

    public HarvesterClientImpl(final MorphiaDataStore datastore, final HarvesterClientConfig harvesterClientConfig) {
        this(new ProcessingJobDaoImpl(datastore.getDatastore()),
                new MachineResourceReferenceDaoImpl(datastore.getDatastore()),
                new SourceDocumentProcessingStatisticsDaoImpl(datastore.getDatastore()),
                new SourceDocumentReferenceDaoImpl(datastore.getDatastore()),
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore.getDatastore()),
                harvesterClientConfig);
    }

    public HarvesterClientImpl(ProcessingJobDao processingJobDao, MachineResourceReferenceDao machineResourceReferenceDao,
                               SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao, HarvesterClientConfig harvesterClientConfig) {

        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.sourceDocumentReferenceMetaInfoDao = sourceDocumentReferenceMetaInfoDao;
        this.harvesterClientConfig = harvesterClientConfig;
    }

    @Override
    public Iterable<com.google.code.morphia.Key<SourceDocumentReference>> createOrModifySourceDocumentReference(List<SourceDocumentReference> sourceDocumentReferences) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        //LOG.debug("Create or modify {} SourceDocumentReferences documents ",sourceDocumentReferences.size());
        final List<MachineResourceReference> machineResourceReferences = new ArrayList<>();

        final List<String> urls = new ArrayList<String>();

        // Resolve all url of IP's
        for (final SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
            urls.add(sourceDocumentReference.getUrl());
        }

        final Map<String, String> urlToIpMap = HarvesterClientHelper.resolveIpsOfUrls(urls);

        // Prepare all the machine references
        for (final SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
            if (!urlToIpMap.containsKey(sourceDocumentReference.getUrl())) {
                throw new MalformedURLException("Cannot solve the IP of the url {}" + sourceDocumentReference.getUrl());
            } else {
                machineResourceReferences.add(new MachineResourceReference(urlToIpMap.get(sourceDocumentReference.getUrl())));
            }
        }

        // Persist everything.
        machineResourceReferenceDao.createOrModify(machineResourceReferences, harvesterClientConfig.getWriteConcern());
        return sourceDocumentReferenceDao.createOrModify(sourceDocumentReferences, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJob) {
        return processingJobDao.createOrModify(processingJob, harvesterClientConfig.getWriteConcern());
    }

    @Override
    public Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(List<ProcessingJob> processingJobs) {
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
    public List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state) {
        throw new NotImplementedException();
    }

    @Override
    public ProcessingJob retrieveProcessingJob(String jobId) {
        return processingJobDao.read(jobId);
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
    public void setActive(String recordID, Boolean active) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        final List<SourceDocumentReference> sourceDocumentReferenceList
                = sourceDocumentReferenceDao.findByRecordID(recordID);
        final List<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatisticsList
                = sourceDocumentProcessingStatisticsDao.findByRecordID(recordID);

        final List<SourceDocumentReference> newSourceDocumentReferenceList = new ArrayList<>();

        for (final SourceDocumentReference sourceDocumentReference : sourceDocumentReferenceList) {
            final SourceDocumentReference newSourceDocumentReference = sourceDocumentReference.withActive(active);
            newSourceDocumentReferenceList.add(newSourceDocumentReference);
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
        return sourceDocumentReferenceMetaInfoDao.update(sourceDocumentReferenceMetaInfo, WriteConcern.NORMAL);
    }

    @Override
    public void updateSourceDocumentProcesssingStatisticsForUrl(String url) {

        final HashFunction hf = Hashing.md5();
        final HashCode hc = hf.newHasher()
                .putString(url, Charsets.UTF_8)
                .hash();
        final String id = hc.toString();

        SourceDocumentProcessingStatistics s = this.sourceDocumentProcessingStatisticsDao.read(id);
        if (s != null) {
            this.sourceDocumentProcessingStatisticsDao.update(s.withActive(true), WriteConcern.NORMAL);
        }

    }

}
