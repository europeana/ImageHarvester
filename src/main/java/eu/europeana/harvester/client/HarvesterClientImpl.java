package eu.europeana.harvester.client;

import eu.europeana.harvester.db.*;
import eu.europeana.harvester.domain.*;

import java.util.List;

public class HarvesterClientImpl implements HarvesterClient {

    private final ProcessingJobDao processingJobDao;

    private final MachineResourceReferenceDao machineResourceReferenceDao;

    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    private final LinkCheckLimitsDao linkCheckLimitsDao;

    public HarvesterClientImpl(ProcessingJobDao processingJobDao, MachineResourceReferenceDao machineResourceReferenceDao,
                               SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               LinkCheckLimitsDao linkCheckLimitsDao) {

        this.processingJobDao = processingJobDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.linkCheckLimitsDao = linkCheckLimitsDao;
    }

    @Override
    public void createOrModifyLinkCheckLimits(LinkCheckLimits linkCheckLimits) {
        System.out.println("Create or modify link check limits");

        linkCheckLimitsDao.createOrModify(linkCheckLimits);
    }

    @Override
    public void createOrModifyProcessingLimits(MachineResourceReference machineResourceReference) {
        System.out.println("Create or modify processing limits");

        machineResourceReferenceDao.createOrModify(machineResourceReference);
    }

    @Override
    public void createOrModifySourceDocumentReference(List<SourceDocumentReference> sourceDocumentReferences) {
        System.out.println("Create or modify SourceDocumentReferences");

        for(SourceDocumentReference sourceDocumentReference : sourceDocumentReferences) {
            sourceDocumentReferenceDao.createOrModify(sourceDocumentReference);
        }
    }

    @Override
    public ProcessingJob createProcessingJob(ProcessingJob processingJob) {
        System.out.println("Create processing job");

        processingJobDao.create(processingJob);
        return null;
    }

    @Override
    public ProcessingJob createProcessingJobForCollection(String collectionId, DocumentReferenceTaskType type) {
        return null;
    }

    @Override
    public ProcessingJob createProcessingJobForRecord(String recordId, DocumentReferenceTaskType type) {
        return null;
    }

    @Override
    public ProcessingJob stopJob(String jobId) {
        return null;
    }

    @Override
    public ProcessingJob startJob(String jobId) {
        return null;
    }

    @Override
    public List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state) {
        return null;
    }

    @Override
    public ProcessingJobStats statsOfJob(String jobId) {
        return null;
    }

}
