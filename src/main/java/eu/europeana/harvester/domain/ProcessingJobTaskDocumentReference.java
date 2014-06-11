package eu.europeana.harvester.domain;

public class ProcessingJobTaskDocumentReference implements ProcessingJobTask {

    final private DocumentReferenceTaskType taskType;

    final private String sourceDocumentReference;

    public ProcessingJobTaskDocumentReference() {
        this.taskType = null;
        this.sourceDocumentReference = null;
    }

    public ProcessingJobTaskDocumentReference(DocumentReferenceTaskType taskType, String sourceDocumentReference) {
        this.taskType = taskType;
        this.sourceDocumentReference = sourceDocumentReference;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public String getSourceDocumentReference() {
        return sourceDocumentReference;
    }
}
