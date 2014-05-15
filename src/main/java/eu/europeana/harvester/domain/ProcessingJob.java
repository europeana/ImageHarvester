package eu.europeana.harvester.domain;

import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * A specific processing job. Contains references to all links that are processed as part of the job.
 */
public class ProcessingJob {

    @Id
    @Property("id")
    private final String id;
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
    private final List<String> sourceDocumentReferences;

    /**
     * The state of the processing job. Indicates an aggregate state of all the links in the job.
     */
    private final JobState state;

    public ProcessingJob() {
        this.state = null;
        this.id = null;
        this.expectedStartDate = null;
        this.providerId = null;
        this.collectionId = null;
        this.recordId = null;
        this.sourceDocumentReferences = null;
    }

    public ProcessingJob(Date expectedStartDate, Long providerId, Long collectionId, Long recordId,
                         List<String> sourceDocumentReferences, JobState state) {
        this.id = UUID.randomUUID().toString();
        this.expectedStartDate = expectedStartDate;
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.sourceDocumentReferences = sourceDocumentReferences;
        this.state = state;
    }

    public ProcessingJob(String id, Date expectedStartDate, Long providerId, Long collectionId, Long recordId,
                         List<String> sourceDocumentReferences, JobState state) {
        this.id = id;
        this.expectedStartDate = expectedStartDate;
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.sourceDocumentReferences = sourceDocumentReferences;
        this.state = state;
    }

    public String getId() {
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

    public List<String> getSourceDocumentReferences() {
        return sourceDocumentReferences;
    }

    public JobState getState() {
        return state;
    }

    public ProcessingJob withState(JobState state) {
        return new ProcessingJob(id, expectedStartDate, providerId, collectionId, recordId,
                sourceDocumentReferences, state);
    }
}
