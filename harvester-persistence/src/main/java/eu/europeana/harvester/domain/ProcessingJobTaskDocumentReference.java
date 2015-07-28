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

    final private ProcessingStatus taskStatus;

    final private ProcessingJobSubTaskStats processingJobSubTaskStats;

    /**
     * List of subtask
     */
    final private List<ProcessingJobSubTask> processingTasks;

    public ProcessingJobTaskDocumentReference() {
        this.taskType = null;
        this.sourceDocumentReferenceID = null;
        this.processingTasks = null;
        this.taskStatus = null;
        this.processingJobSubTaskStats = null;
    }

    public ProcessingJobTaskDocumentReference(final DocumentReferenceTaskType taskType,
                                              final String sourceDocumentReferenceID,
                                              List<ProcessingJobSubTask> processingTasks) {
        this.taskType = taskType;
        this.sourceDocumentReferenceID = sourceDocumentReferenceID;
        this.processingTasks = processingTasks;
        this.taskStatus = ProcessingStatus.Unknown;
        this.processingJobSubTaskStats = null;
    }

     public ProcessingJobTaskDocumentReference(final DocumentReferenceTaskType taskType,
                                               final ProcessingStatus taskStatus,
                                               final ProcessingJobSubTaskStats processingJobSubTaskStats,
                                               final String sourceDocumentReferenceID,
                                               List<ProcessingJobSubTask> processingTasks) {
        this.taskType = taskType;
        this.sourceDocumentReferenceID = sourceDocumentReferenceID;
        this.processingTasks = processingTasks;
        this.taskStatus = taskStatus;
        this.processingJobSubTaskStats = processingJobSubTaskStats;
    }


    public ProcessingJobSubTaskStats computeProcessingJobSubTasksStats() {
        ProcessingJobSubTaskState retreive = (null == processingJobSubTaskStats) ? null : processingJobSubTaskStats.getRetrieveState();
        ProcessingJobSubTaskState colorExtraction = null;
        ProcessingJobSubTaskState metaInfoExtraction = null;
        ProcessingJobSubTaskState thumbnailGeneration = null;
        ProcessingJobSubTaskState thumbnailStorage = (null == processingJobSubTaskStats) ? null : processingJobSubTaskStats.getThumbnailStorageState();

        for (final ProcessingJobSubTask subTask: processingTasks) {
            switch (subTask.getTaskType()) {
                case COLOR_EXTRACTION:  colorExtraction = changeState(colorExtraction, subTask.getTaskState()); break;
                case GENERATE_THUMBNAIL: thumbnailGeneration = changeState(thumbnailGeneration, subTask.getTaskState()); break;
                case META_EXTRACTION: metaInfoExtraction = changeState(metaInfoExtraction, subTask.getTaskState()); break;

            }
        }

        return new ProcessingJobSubTaskStats(retreive,
                                             colorExtraction,
                                             metaInfoExtraction,
                                             thumbnailGeneration,
                                             thumbnailStorage
                                            );
    }

    private ProcessingJobSubTaskState changeState(ProcessingJobSubTaskState oldState, ProcessingJobSubTaskState newState) {
        if (null == oldState) return newState;
        switch (newState) {
            case ERROR: return newState;
            case READY: return (ProcessingJobSubTaskState.ERROR == oldState) ? oldState: newState;
            case SUCCESS: return (ProcessingJobSubTaskState.SUCCESS != oldState) ? oldState: newState;
            default: throw new IllegalArgumentException();
        }
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

    public ProcessingStatus getTaskStatus () {
        return taskStatus;
    }

    public ProcessingJobTaskDocumentReference withTaskState () {
        ProcessingStatus state = null;

        for (final ProcessingJobSubTask subTask: processingTasks) {
            switch (subTask.getTaskState()) {
                case ERROR:  state = ProcessingStatus.Failure; break;

                //set to success only if the state wasn't set before
                case SUCCESS: state = (null == state) ? ProcessingStatus.Success: state; break;

                case READY:  state = ProcessingStatus.Unknown; break;
            }

            if (ProcessingStatus.Failure == state) break;
        }

        return new ProcessingJobTaskDocumentReference(taskType, state, processingJobSubTaskStats, sourceDocumentReferenceID, processingTasks);
    }

    public ProcessingJobTaskDocumentReference withTaskState(ProcessingStatus status) {
        return new ProcessingJobTaskDocumentReference(taskType, status, processingJobSubTaskStats, sourceDocumentReferenceID, processingTasks);
    }


    public ProcessingJobTaskDocumentReference withProcessingJobSubTaskStats() {
        return new ProcessingJobTaskDocumentReference(taskType,
                                                      taskStatus,
                                                      computeProcessingJobSubTasksStats(),
                                                      sourceDocumentReferenceID, processingTasks);
    }

    public ProcessingJobTaskDocumentReference withProcessingJobSubTaskStats(ProcessingJobSubTaskStats stats) {
        return new ProcessingJobTaskDocumentReference(taskType, taskStatus, stats, sourceDocumentReferenceID, processingTasks);
    }

    public ProcessingJobSubTaskStats getProcessingJobSubTaskStats () {
        return processingJobSubTaskStats;
    }
}
