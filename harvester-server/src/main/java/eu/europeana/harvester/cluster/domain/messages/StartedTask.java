package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Message sent by slaves to notice the master actor that the download has started.
 */
public class StartedTask implements Serializable {

    /**
     * The unique ID of the task.
     */
    private final String taskID;

    public StartedTask(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
