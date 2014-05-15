package eu.europeana.harvester.client;

import eu.europeana.harvester.domain.*;

/**
 * The public interface to the harvester client. Used by any external system to control the harvester cluster.
 */
interface HarvesterClient {

    public void createOrModifyLinkCheckLimits(final LinkCheckLimits linkCheckLimits);

    public void createOrModifyProcessingLimits(final ProcessingLimits processingLimits);

    public void createProcessingJob(final ProcessingJob processingJob);

    public void createOrModifySourceDocumentReference(final SourceDocumentReference sourceDocumentReference);

    public CollectionStats statsOfCollection(Long collectionId);

    public void stopCollection(Long collectionId);

    public void startCollection(Long collectionId);
}

