package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetTask implements Serializable {

    private final String taskID;

    public GetTask(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
