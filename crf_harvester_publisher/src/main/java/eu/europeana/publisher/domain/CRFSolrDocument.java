package eu.europeana.publisher.domain;

import java.util.List;

public class CRFSolrDocument {

    /**
     * Unique ID of the resource
     */
    private final String recordId;

    /**
     * True if it is a text document.
     */
    private final Boolean isFulltext;

    /**
     * True if it has thumbnails
     */
    private final Boolean hasThumbnails;

    /**
     * True if it is image, sound or video
     */
    private final Boolean hasMedia;

    /**
     * The list of all “fake tags” combinations for a particular CHO
     */
    private final List<Integer> filterTags;

    /**
     * The list of all “fake tags” pure (ie. colour red, colour green, small size, etc.) for a particular CHO
     */
    private final List<Integer> facetTags;

    public CRFSolrDocument(final String recordId, final Boolean isFulltext, final Boolean hasThumbnails,
                           final Boolean hasMedia, final List<Integer> filterTags, final List<Integer> facetTags) {
        this.recordId = recordId;
        this.isFulltext = isFulltext;
        this.hasThumbnails = hasThumbnails;
        this.hasMedia = hasMedia;
        this.filterTags = filterTags;
        this.facetTags = facetTags;
    }

    public String getRecordId() {
        return recordId;
    }

    public Boolean getIsFulltext() {
        return isFulltext;
    }

    public Boolean getHasThumbnails() {
        return hasThumbnails;
    }

    public Boolean getHasMedia() {
        return hasMedia;
    }

    public List<Integer> getFilterTags() {
        return filterTags;
    }

    public List<Integer> getFacetTags() {
        return facetTags;
    }

}
