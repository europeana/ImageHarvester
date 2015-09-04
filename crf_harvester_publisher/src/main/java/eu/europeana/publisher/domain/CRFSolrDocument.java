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
    private final boolean isFullText;

    /**
     * True if it has thumbnails
     */
    private final boolean hasThumbnails;

    /**
     * True if it is image, sound or video
     */
    private final boolean hasMedia;

    private final boolean hasLandingPage;

    private final String provider_edm_object;

    /**
     * The list of all “fake tags” combinations for a particular CHO
     */
    private final List<Integer> filterTags;

    /**
     * The list of all “fake tags” pure (ie. colour red, colour green, small size, etc.) for a particular CHO
     */
    private final List<Integer> facetTags;

    public CRFSolrDocument (final String recordId) {
        this.recordId = recordId;

        facetTags = null;
        filterTags = null;
        provider_edm_object = null;
        hasMedia = false;
        hasThumbnails = false;
        hasLandingPage = false;
        isFullText = false;
    }

    public CRFSolrDocument (final String recordId,
                            final boolean isFullText,
                            final boolean hasThumbnails,
                            final boolean hasMedia,
                            final boolean hasLandingPage,
                            final List<Integer> filterTags,
                            final List<Integer> facetTags,
                            final String provider_edm_object
                           ) {
        this.recordId = recordId;
        this.isFullText = isFullText;
        this.hasThumbnails = hasThumbnails;
        this.hasMedia = hasMedia;
        this.filterTags = filterTags;
        this.facetTags = facetTags;
        this.hasLandingPage = hasLandingPage;
        this.provider_edm_object = provider_edm_object;
    }

    public String getRecordId() {
        return recordId;
    }

    public Boolean getIsFullText () {
        return isFullText;
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

    public Boolean getHasLandingPage () {
        return hasLandingPage;
    }

    public String getProviderEdmObject() {return provider_edm_object;}

    public CRFSolrDocument withIsFullText(boolean isFullText) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
                                   );
    }

    public CRFSolrDocument withHasThumbnails (boolean hasThumbnails) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
        );
    }

    public CRFSolrDocument withHasMedia (boolean hasMedia) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
        );
    }

    public CRFSolrDocument withHasLandingpage (boolean hasLandingPage) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
        );
    }

    public CRFSolrDocument withFilterTags (final List<Integer> filterTags) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
        );
    }

    public CRFSolrDocument withFacetTags (final List<Integer> facetTags) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
        );
    }

    public CRFSolrDocument withProviderEdmObject (final String provider_edm_object) {
        return new CRFSolrDocument(recordId,
                                   isFullText,
                                   hasThumbnails,
                                   hasMedia,
                                   hasLandingPage,
                                   filterTags,
                                   facetTags,
                                   provider_edm_object
        );
    }
}
