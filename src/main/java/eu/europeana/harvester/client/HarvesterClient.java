package eu.europeana.harvester.client;

import eu.europeana.harvester.domain.*;

import java.util.List;

/**
 * The public interface to the harvester client. Used by any external system to control the harvester cluster.
 */
interface HarvesterClient {

    public void createOrModifyLinkCheckLimits(final LinkCheckLimits linkCheckLimits);

    public void createOrModifyProcessingLimits(final MachineResourceReference processingLimits);

    public void createOrModifySourceDocumentReference(final List<SourceDocumentReference> sourceDocumentReference);

    public ProcessingJob createProcessingJob(final ProcessingJob processingJob);

    public ProcessingJob createProcessingJobForCollection(String collectionId, DocumentReferenceTaskType type);

    public ProcessingJob createProcessingJobForRecord(String recordId, DocumentReferenceTaskType type);

    public ProcessingJob stopJob(String jobId);

    public ProcessingJob startJob(String jobId);

    public List<ProcessingJob> findJobsByCollectionAndState(String collectionId, List<ProcessingState> state);

    public ProcessingJobStats statsOfJob(String jobId);

}

