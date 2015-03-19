package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;
import java.util.List;

public class AddTasksToJob implements Serializable {

    private final String jobID;

    private final List<String> taskIDs;

    public AddTasksToJob(String jobID, List<String> taskIDs) {
        this.jobID = jobID;
        this.taskIDs = taskIDs;
    }

    public String getJobID() {
        return jobID;
    }

    public List<String> getTaskIDs() {
        return taskIDs;
    }
}
