package eu.europeana.harvester.domain;

import org.mongodb.morphia.annotations.Id;
import org.mongodb.morphia.annotations.Property;

import java.util.UUID;

/**
 * Represents the reference of a source document.
 */
public class SourceDocumentReference {

    /**
     * The id of the link. Used for storage identity/uniqueness.
     */
    @Id
    @Property("id")
	private final String id;

    /**
     * The provider that owns the link. When null it's not owned by any provider.
     */
    private final Long providerId;

    /**
     * The collection that owns the link. When null it's not owned by any collection.
     */
    private final Long collectionId;

    /**
     * The record that owns the link. When null it's not owned by any record.
     */
    private final Long recordId;

    /**
     * The url.
     */
	private final String url;

    /**
     * Whether to check if the link exists.
     */
    private final Boolean checked;

    /**
     * Whether to download the link.
     */
    private final Boolean retrieved;

    /**
     * Whether to debug the link.
     */
    private final Boolean debugged;

    public SourceDocumentReference() {
        this.id = null;
        this.providerId = null;
        this.collectionId = null;
        this.recordId = null;
        this.url = null;
        this.checked = null;
        this.retrieved = null;
        this.debugged = null;
    }

    public SourceDocumentReference(Long providerId, Long collectionId, Long recordId, String url, Boolean checked,
                                   Boolean retrieved, Boolean debugged) {
        this.id = UUID.randomUUID().toString();
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.url = url;
        this.checked = checked;
        this.retrieved = retrieved;
        this.debugged = debugged;
    }

    public SourceDocumentReference(String id, Long providerId, Long collectionId, Long recordId, String url,
                                   Boolean checked, Boolean retrieved, Boolean debugged) {
        this.id = id;
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.url = url;
        this.checked = checked;
        this.retrieved = retrieved;
        this.debugged = debugged;
    }

    public String getId() {
        return id;
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

    public String getUrl() {
        return url;
    }

    public Boolean getChecked() {
        return checked;
    }

    public Boolean getRetrieved() {
        return retrieved;
    }

    public Boolean getDebugged() {
        return debugged;
    }
}
