package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetTaskStatesPerJob implements Serializable {

    private final String jobID;

    public GetTaskStatesPerJob(String jobID) {
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
