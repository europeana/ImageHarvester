package eu.europeana.harvester.domain;

import java.util.Date;
import java.util.List;

/**
 * A specific processing job. Contains references to all links that are processed as part of the job.
 */
public class ProcessingJob {
    private final Long id;
    /**
     * The expected start date.
     */
    private final Date expectedStartDate;

    /**
     * The provider this job refers to. Useful for querying and stats.
     */
    private final Long providerId;

    /**
     * The collection this job refers to. Useful for querying and stats.
     */
    private final Long collectionId;

    /**
     * The record this job refers to. Useful for querying and stats.
     */
    private final Long recordId;

    /**
     * The individual source references that have to be processed as part of the job.
     */
    private final List<Long> sourceDocumentReferences;

    public ProcessingJob(Long id, Date expectedStartDate, Long providerId, Long collectionId, Long recordId, List<Long> sourceDocumentReferences) {
        this.id = id;
        this.expectedStartDate = expectedStartDate;
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.sourceDocumentReferences = sourceDocumentReferences;
    }

    public Long getId() {
        return id;
    }

    public Date getExpectedStartDate() {
        return expectedStartDate;
    }

    public Long getProviderId() {
        return providerId;
    }

    public Long getCollectionId() {
        return collectionId;
    }

    public Long getRecordId() {
        return recordId;
    }

    public List<Long> getSourceDocumentReferences() {
        return sourceDocumentReferences;
    }
}
