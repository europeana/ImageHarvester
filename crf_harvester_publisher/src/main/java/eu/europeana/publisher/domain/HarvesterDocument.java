package eu.europeana.publisher.domain;

import eu.europeana.harvester.domain.*;
import org.joda.time.DateTime;

public class HarvesterDocument {

    private final String sourceDocumentReferenceId;

    private final DateTime updatedAt;

    private final ReferenceOwner referenceOwner;

    private final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo;

    private final ProcessingJobSubTaskStats subTaskStats;

    private final URLSourceType urlSourceType;

    private final DocumentReferenceTaskType taskType;

    private final String url;

    public HarvesterDocument (String sourceDocumentReferenceId, DateTime updatedAt, ReferenceOwner referenceOwner,
                              SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo,
                              ProcessingJobSubTaskStats subTaskStats, URLSourceType urlSourceType,
                              DocumentReferenceTaskType taskType,
                              String url) {
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.updatedAt = updatedAt;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceMetaInfo = sourceDocumentReferenceMetaInfo;
        this.subTaskStats = subTaskStats;
        this.urlSourceType = urlSourceType;
        this.url = url;
        this.taskType = taskType;
    }

    public HarvesterDocument (String sourceDocumentReferenceId, DateTime updatedAt, ReferenceOwner referenceOwner,
                              SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo,
                              ProcessingJobSubTaskStats subTaskStats, URLSourceType urlSourceType,
                              DocumentReferenceTaskType taskType
                              ) {
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.updatedAt = updatedAt;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceMetaInfo = sourceDocumentReferenceMetaInfo;
        this.subTaskStats = subTaskStats;
        this.urlSourceType = urlSourceType;
        this.url = null;
        this.taskType = taskType;
    }

    public ProcessingJobSubTaskStats getSubTaskStats() {return subTaskStats;}

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
        return new HarvesterDocument(sourceDocumentReferenceId, updatedAt, referenceOwner, newSourceDocumentReferenceMetaInfo,
                                     subTaskStats, urlSourceType, taskType, url);
    }

    public URLSourceType getUrlSourceType () {
        return urlSourceType;
    }

    public String getUrl () {
        return url;
    }

    public DocumentReferenceTaskType getTaskType () {
        return taskType;
    }

    public HarvesterDocument withUrl(final String url) {
        return new HarvesterDocument(sourceDocumentReferenceId,
                                     updatedAt,
                                     referenceOwner,
                                     sourceDocumentReferenceMetaInfo,
                                     subTaskStats,
                                     urlSourceType,
                                     taskType,
                                     url
        );
    }
}
