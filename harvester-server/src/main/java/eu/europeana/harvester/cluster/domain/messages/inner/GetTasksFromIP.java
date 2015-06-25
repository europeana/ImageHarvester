package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.domain.JobPriority;

import java.io.Serializable;

public class GetTasksFromIP implements Serializable {

    private final String IP;
    private final JobPriority jobPriority;

    public GetTasksFromIP(String ip, JobPriority jobPriority ) {

        this.IP = ip;
        this.jobPriority = jobPriority;
    }

    public String getIP() {
        return IP;
    }
    public JobPriority getJobPriority() { return jobPriority; }

}
