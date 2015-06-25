package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.domain.JobPriority;

import java.io.Serializable;
import java.util.List;

public class AddTasksToJob implements Serializable {

    private final String jobID;
    private final Integer jobPriority;

    private final List<String> taskIDs;

    public AddTasksToJob(String jobID, Integer jobPriority, List<String> taskIDs) {
        this.jobID = jobID;
        this.jobPriority = jobPriority;
        this.taskIDs = taskIDs;
    }

    public String getJobID() {
        return jobID;
    }

    public Integer getJobPriority() { return jobPriority; }

    public List<String> getTaskIDs() {
        return taskIDs;
    }
}
