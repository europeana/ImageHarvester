package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class RemoveJob implements Serializable {

    private final String jobID;

    public RemoveJob(String jobID) {
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
