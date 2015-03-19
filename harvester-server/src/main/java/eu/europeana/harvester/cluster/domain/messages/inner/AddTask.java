package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.utils.Pair;

import java.io.Serializable;

public class AddTask implements Serializable {

    private final String taskID;

    private final Pair<RetrieveUrl, TaskState> taskWithState;

    public AddTask(String taskID, Pair<RetrieveUrl, TaskState> taskWithState) {
        this.taskID = taskID;
        this.taskWithState = taskWithState;
    }

    public String getTaskID() {
        return taskID;
    }

    public Pair<RetrieveUrl, TaskState> getTaskWithState() {
        return taskWithState;
    }
}
