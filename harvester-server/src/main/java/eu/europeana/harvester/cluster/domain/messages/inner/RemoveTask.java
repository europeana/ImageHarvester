package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class RemoveTask implements Serializable {

    private final String taskID;

    public RemoveTask(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
