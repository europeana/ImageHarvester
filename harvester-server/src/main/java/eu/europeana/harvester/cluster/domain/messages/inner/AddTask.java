package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.domain.JobPriority;

import java.io.Serializable;

public class AddTask implements Serializable {

    private final String taskID;

    private final Integer jobPriority;

    private final Pair<RetrieveUrl, TaskState> taskWithState;

    public AddTask(Integer jobPriority, String taskID, Pair<RetrieveUrl, TaskState> taskWithState) {
        this.taskID = taskID;
        this.jobPriority = jobPriority;
        this.taskWithState = taskWithState;
    }

    public String getTaskID() {
        return taskID;
    }
    public Integer getJobPriority() { return jobPriority; }
    public Pair<RetrieveUrl, TaskState> getTaskWithState() {
        return taskWithState;
    }
}
