package eu.europeana.harvester.domain;

import java.io.Serializable;

/**
 * The wrapping class of a subtask
 */
public class ProcessingJobSubTask implements Serializable {

    /**
     * The type of the subtask. (COLOR_EXTRACTION, META_EXTRACTION or GENERATE_THUMBNAIL)
     */
    private final ProcessingJobSubTaskType taskType;

    private final ProcessingJobSubTaskState taskState;

    /**
     * The configuration object needed by the specified subtask.
     */
    private GenericSubTaskConfiguration config;

    public ProcessingJobSubTask(ProcessingJobSubTaskType taskType,
                                GenericSubTaskConfiguration config) {
        this.taskType = taskType;
        this.config = config;
        this.taskState = ProcessingJobSubTaskState.READY;
    }


    public ProcessingJobSubTask(ProcessingJobSubTaskType taskType,
                                GenericSubTaskConfiguration config,
                                ProcessingJobSubTaskState taskState) {
        this.taskType = taskType;
        this.config = config;
        this.taskState = taskState;
    }

    public ProcessingJobSubTask() {
        taskState = null;
        config = null;
        taskType = null;
    }

    public ProcessingJobSubTaskType getTaskType() {
        return taskType;
    }

    public GenericSubTaskConfiguration getConfig() {
        return config;
    }

    @Override
    public boolean equals (Object obj) {
        if (null == obj || !(obj instanceof ProcessingJobSubTask)) {
            return false;
        }

        final ProcessingJobSubTask task = (ProcessingJobSubTask)obj;
        return this.taskType == task.getTaskType() &&
               (null == config ? null == task.getConfig() : config.equals(task.getConfig())) &&
               taskType == task.getTaskType();
    }

    public ProcessingJobSubTaskState getTaskState () {
        return taskState;
    }
}
