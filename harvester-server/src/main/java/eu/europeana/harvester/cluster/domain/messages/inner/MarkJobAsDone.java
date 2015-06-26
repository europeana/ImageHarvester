package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class MarkJobAsDone implements Serializable {

    private final String jobID;


    public MarkJobAsDone(String jobID) {

        this.jobID = jobID;

    }

    public String getJobID() {
        return jobID;
    }

}
