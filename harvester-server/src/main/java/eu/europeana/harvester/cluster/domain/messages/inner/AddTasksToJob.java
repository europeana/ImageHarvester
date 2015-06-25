package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.domain.JobPriority;

import java.io.Serializable;
import java.util.List;

public class AddTasksToJob implements Serializable {

    private final String jobID;
    private final JobPriority jobPriority;

    private final List<String> taskIDs;

    public AddTasksToJob(String jobID, JobPriority jobPriority, List<String> taskIDs) {
        this.jobID = jobID;
        this.jobPriority = jobPriority;
        this.taskIDs = taskIDs;
    }

    public String getJobID() {
        return jobID;
    }

    public JobPriority getJobPriority() { return jobPriority; }

    public List<String> getTaskIDs() {
        return taskIDs;
    }
}
