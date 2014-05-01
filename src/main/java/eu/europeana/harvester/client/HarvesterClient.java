package eu.europeana.harvester.client;

import eu.europeana.harvester.domain.CollectionStats;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.ProcessingLimits;
import eu.europeana.harvester.domain.SourceDocumentReference;

/**
 * The public interface to the harvester client. Used by any external system to control the harvester cluster.
 */
public interface HarvesterClient {

    public void createOrModifyProcessingLimits(final ProcessingLimits processingLimits);

    public void createProcessingJob(final ProcessingJob processingJob);

    public void createOrModifySourceDocumentReference(final SourceDocumentReference sourceDocumentReference);

    public CollectionStats statsOfCollection(Long collectionId);

    public void stopCollection(Long collectionId);

    public void startCollection(Long collectionId);
}

