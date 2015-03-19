package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetTaskState implements Serializable {

    private final String taskID;

    public GetTaskState(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
