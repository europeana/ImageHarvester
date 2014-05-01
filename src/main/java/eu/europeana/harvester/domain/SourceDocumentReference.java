package eu.europeana.harvester.domain;

/**
 * Represents the reference of a source document.
 */
public class SourceDocumentReference {

    /**
     * The id of the link. Used for storage identity/uniqueness.
     */
	private final Long id;

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

    public SourceDocumentReference(Long id, Long providerId, Long collectionId, Long recordId, String url, Boolean checked, Boolean retrieved, Boolean debugged) {
        this.id = id;
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.url = url;
        this.checked = checked;
        this.retrieved = retrieved;
        this.debugged = debugged;
    }
}
