package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.domain.JobPriority;

import java.io.Serializable;

public class AddTask implements Serializable {

    private final String taskID;

    private final JobPriority jobPriority;

    private final Pair<RetrieveUrl, TaskState> taskWithState;

    public AddTask(JobPriority jobPriority, String taskID, Pair<RetrieveUrl, TaskState> taskWithState) {
        this.taskID = taskID;
        this.jobPriority = jobPriority;
        this.taskWithState = taskWithState;
    }

    public String getTaskID() {
        return taskID;
    }
    public JobPriority getJobPriority() { return jobPriority; }
    public Pair<RetrieveUrl, TaskState> getTaskWithState() {
        return taskWithState;
    }
}
