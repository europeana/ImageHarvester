package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.io.Serializable;
import java.util.Date;

public class SourceDocumentReferenceProcessingProfile implements Serializable {


    public final static String idFromUrl(final String sourceDocumentReferenceId, final URLSourceType urlSourceType, final DocumentReferenceTaskType taskType) {
        return new StringBuilder().append(sourceDocumentReferenceId).append("-").append(urlSourceType.name())
                                  .append("-").append(taskType.name()).toString();
    }

    /**
     * The id of the link. Used for storage identity/uniqueness.
     * It is computed by a concatenation of sourceDocumentReferenceId, urlSourceType and taskType as this
     * insures that the number of profiles don't grow too much.
     */
    @Id
    @Property("id")
    private final String id;

    private final Boolean active;

    /**
     * An object which contains: provider id, collection id, record id
     */
    private final ReferenceOwner referenceOwner;

    /**
     * The URL for which the processing job needs to be created.
     */
    private final String sourceDocumentReferenceId;

    /**
     * The type of field in the EDM that contains that URL.
     * We store this separately from the SourceDocumentReference as the same URL can appear
     * in different EDM fields.
     */
    private final URLSourceType urlSourceType;

    /**
     * The task that will be executed by the processing job.
     */
    private final DocumentReferenceTaskType taskType;

    /**
     * The priority of the processing jobs created.
     */
    private final int priority;

    /**
     * The future date when this processing profile needs to be evaluated.
     * Useful for efficient future scheduling & detection of lagging behind.
     */
    private final Date toBeEvaluatedAt;

    /**
     * The number of seconds between two successive evaluations of the profile.
     * It's the only way to configure the frequency of the evaluation.
     */
    private final long secondsBetweenEvaluations;

    public SourceDocumentReferenceProcessingProfile(final Boolean active,
                                                    ReferenceOwner referenceOwner,
                                                    String sourceDocumentReferenceId,
                                                    URLSourceType urlSourceType,
                                                    DocumentReferenceTaskType taskType,
                                                    int priority,
                                                    Date toBeEvaluatedAt,
                                                    long secondsBetweenEvaluations) {
        this.id = idFromUrl(sourceDocumentReferenceId, urlSourceType, taskType);
        this.active = active;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.urlSourceType = urlSourceType;
        this.taskType = taskType;
        this.priority = priority;
        this.toBeEvaluatedAt = toBeEvaluatedAt;
        this.secondsBetweenEvaluations = secondsBetweenEvaluations;
    }

    public SourceDocumentReferenceProcessingProfile(String id,final Boolean active, ReferenceOwner referenceOwner, String sourceDocumentReferenceId, URLSourceType urlSourceType, DocumentReferenceTaskType taskType, int priority, Date toBeEvaluatedAt, long secondsBetweenEvaluations) {
        this.id = id;
        this.active = active;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.urlSourceType = urlSourceType;
        this.taskType = taskType;
        this.priority = priority;
        this.toBeEvaluatedAt = toBeEvaluatedAt;
        this.secondsBetweenEvaluations = secondsBetweenEvaluations;
    }

    public String getId() {
        return id;
    }

    public Boolean getActive() {
        return active;
    }

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public String getSourceDocumentReferenceId() {
        return sourceDocumentReferenceId;
    }

    public URLSourceType getUrlSourceType() {
        return urlSourceType;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public int getPriority() {
        return priority;
    }

    public Date getToBeEvaluatedAt() {
        return toBeEvaluatedAt;
    }

    public long getSecondsBetweenEvaluations() {
        return secondsBetweenEvaluations;
    }
}
