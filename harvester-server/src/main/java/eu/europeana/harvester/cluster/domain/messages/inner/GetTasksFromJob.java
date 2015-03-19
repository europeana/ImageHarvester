package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetTasksFromJob implements Serializable {

    private final String jobID;

    public GetTasksFromJob(String jobID) {
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
