package eu.europeana.harvester.domain;

import java.io.Serializable;
import java.util.List;

/**
 * An object which contains references to all the important information about a task.
 */
public class ProcessingJobTaskDocumentReference implements ProcessingJobTask, Serializable {

    /**
     * The type of the task.
     */
    final private DocumentReferenceTaskType taskType;

    /**
     * The if of a document.
     */
    final private String sourceDocumentReferenceID;


    /**
     * List of subtask
     */
    final private List<ProcessingJobSubTask> processingTasks;

    public ProcessingJobTaskDocumentReference() {
        this.taskType = null;
        this.sourceDocumentReferenceID = null;
        this.processingTasks = null;
    }

    public ProcessingJobTaskDocumentReference(final DocumentReferenceTaskType taskType,
                                              final String sourceDocumentReferenceID,
                                              List<ProcessingJobSubTask> processingTasks) {
        this.taskType = taskType;
        this.sourceDocumentReferenceID = sourceDocumentReferenceID;
        this.processingTasks = processingTasks;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public String getSourceDocumentReferenceID() {
        return sourceDocumentReferenceID;
    }

    public List<ProcessingJobSubTask> getProcessingTasks() {
        return processingTasks;
    }
}
