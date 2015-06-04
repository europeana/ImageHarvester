package eu.europeana.publisher.domain;

import org.joda.time.DateTime;

/**
 * Auxiliary class
 */
public class RetrievedDoc {

    private final String sourceDocumentReferenceId;

    /**
     * Unique ID of the resource
     */
    private final String recordId;

    private final DateTime updatedAt;

    public RetrievedDoc(String sourceDocumentReferenceId, String recordId,DateTime updatedAt) {
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.recordId = recordId;
        this.updatedAt = updatedAt;
    }

    public String getSourceDocumentReferenceId () {
        return sourceDocumentReferenceId;
    }

    public String getRecordId() {
        return recordId;
    }

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

}
