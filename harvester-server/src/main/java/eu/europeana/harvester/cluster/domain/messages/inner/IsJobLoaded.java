package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class IsJobLoaded implements Serializable {

    private final String jobID;

    public IsJobLoaded(String jobID) {
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
