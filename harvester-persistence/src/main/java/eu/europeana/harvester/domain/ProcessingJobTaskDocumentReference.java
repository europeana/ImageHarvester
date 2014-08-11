package eu.europeana.harvester.domain;

/**
 * An object which contains references to all the important information about a task.
 */
public class ProcessingJobTaskDocumentReference implements ProcessingJobTask {

    /**
     * The type of the task.
     */
    final private DocumentReferenceTaskType taskType;

    /**
     * The if of a document.
     */
    final private String sourceDocumentReferenceID;

    public ProcessingJobTaskDocumentReference() {
        this.taskType = null;
        this.sourceDocumentReferenceID = null;
    }

    public ProcessingJobTaskDocumentReference(final DocumentReferenceTaskType taskType,
                                              final String sourceDocumentReferenceID) {
        this.taskType = taskType;
        this.sourceDocumentReferenceID = sourceDocumentReferenceID;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public String getSourceDocumentReferenceID() {
        return sourceDocumentReferenceID;
    }
}
