package eu.europeana.publisher.domain;

import org.joda.time.DateTime;

/**
 * Auxiliary class
 */
public class RetrievedDoc {

    /**
     * The mimetype of the resource
     */
    private final String type;

    /**
     * Unique ID of the resource
     */
    private final String recordId;

    private final DateTime updatedAt;

    public RetrievedDoc(String type, String recordId,DateTime updatedAt) {
        this.type = type;
        this.recordId = recordId;
        this.updatedAt = updatedAt;
    }

    public String getType() {
        return type;
    }

    public String getRecordId() {
        return recordId;
    }

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

}
