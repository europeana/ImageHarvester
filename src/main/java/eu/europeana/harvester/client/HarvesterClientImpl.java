package eu.europeana.harvester.client;

import eu.europeana.harvester.db.*;
import eu.europeana.harvester.domain.*;

public class HarvesterClientImpl implements HarvesterClient {

    private final ProcessingJobDao processingJobDao;

    private final ProcessingLimitsDao processingLimitsDao;

    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

    private final LinkCheckLimitsDao linkCheckLimitsDao;

    public HarvesterClientImpl(ProcessingJobDao processingJobDao, ProcessingLimitsDao processingLimitsDao,
                               SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                               SourceDocumentReferenceDao sourceDocumentReferenceDao,
                               LinkCheckLimitsDao linkCheckLimitsDao) {

        this.processingJobDao = processingJobDao;
        this.processingLimitsDao = processingLimitsDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.linkCheckLimitsDao = linkCheckLimitsDao;
    }

    @Override
    public void createOrModifyLinkCheckLimits(LinkCheckLimits linkCheckLimits) {
        System.out.println("Create or modify link check limits");

        linkCheckLimitsDao.create(linkCheckLimits);
    }

    @Override
    public void createOrModifyProcessingLimits(ProcessingLimits processingLimits) {
        System.out.println("Create or modify processing limits");

        processingLimitsDao.createOrModify(processingLimits);
    }

    @Override
    public void createProcessingJob(ProcessingJob processingJob) {
        System.out.println("Create processing job");

        processingJobDao.create(processingJob);
    }

    @Override
    public void createOrModifySourceDocumentReference(SourceDocumentReference sourceDocumentReference) {
        System.out.println("Create or modify source document reference");


        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference);
    }

    @Override
    public CollectionStats statsOfCollection(Long collectionId) {
        return null;
    }

    @Override
    public void stopCollection(Long collectionId) {
    }

    @Override
    public void startCollection(Long collectionId) {
    }
}
