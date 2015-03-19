package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class ResumeTasks implements Serializable {

    private final String jobID;

    public ResumeTasks(String jobID) {
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
