package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class RemoveJob implements Serializable {

    private final String jobID;
    private final String IP;

    public RemoveJob(String jobID, String IP) {

        this.jobID = jobID;
        this.IP =IP;
    }

    public String getJobID() {
        return jobID;
    }
    public String getIP(){return IP;}
}
