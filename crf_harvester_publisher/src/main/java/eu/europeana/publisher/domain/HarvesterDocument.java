package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import org.joda.time.DateTime;

public class HarvesterDocument {

    private final String sourceDocumentReferenceId;

    private final DateTime updatedAt;

    private final ReferenceOwner referenceOwner;

    private final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo;

    public HarvesterDocument(String sourceDocumentReferenceId, DateTime updatedAt, ReferenceOwner referenceOwner, SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo) {
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.updatedAt = updatedAt;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceMetaInfo = sourceDocumentReferenceMetaInfo;
    }

    public String getSourceDocumentReferenceId() {
        return sourceDocumentReferenceId;
    }

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public SourceDocumentReferenceMetaInfo getSourceDocumentReferenceMetaInfo() {
        return sourceDocumentReferenceMetaInfo;
    }

    public HarvesterDocument withSourceDocumentReferenceMetaInfo(final SourceDocumentReferenceMetaInfo newSourceDocumentReferenceMetaInfo) {
        return new HarvesterDocument(sourceDocumentReferenceId, updatedAt, referenceOwner, newSourceDocumentReferenceMetaInfo);
    }
}
