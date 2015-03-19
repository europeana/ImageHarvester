package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.cluster.domain.TaskState;

import java.io.Serializable;

public class ModifyState implements Serializable {

    private final String taskID;

    private final TaskState state;

    public ModifyState(String taskID, TaskState state) {
        this.taskID = taskID;
        this.state = state;
    }

    public String getTaskID() {
        return taskID;
    }

    public TaskState getState() {
        return state;
    }
}
