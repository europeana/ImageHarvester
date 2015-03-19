package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class PauseTasks implements Serializable {

    private final String jobID;

    public PauseTasks(String jobID) {
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
