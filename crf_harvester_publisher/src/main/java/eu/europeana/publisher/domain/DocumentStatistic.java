package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import org.joda.time.DateTime;

/**
 * Auxiliary class
 */
public class DocumentStatistic {

    private final String sourceDocumentReferenceId;

    /**
     * Unique ID of the resource
     */
    private final String recordId;

    private final DateTime updatedAt;

    private SourceDocumentReferenceMetaInfo metaInfo;


    public DocumentStatistic (String sourceDocumentReferenceId, String recordId, DateTime updatedAt) {
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.recordId = recordId;
        this.updatedAt = updatedAt;
        this.metaInfo = null;
    }

    public DocumentStatistic (String sourceDocumentReferenceId, String recordId, DateTime updatedAt, SourceDocumentReferenceMetaInfo metaInfo) {
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.recordId = recordId;
        this.updatedAt = updatedAt;
        this.metaInfo = metaInfo;
    }

    public void setMetaInfo (final SourceDocumentReferenceMetaInfo metaInfo) {
        this.metaInfo = metaInfo;
    }

    public SourceDocumentReferenceMetaInfo getMetaInfo() {return metaInfo;}

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
