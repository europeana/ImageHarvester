package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetConcreteTask implements Serializable {

    private final String taskID;

    public GetConcreteTask(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
