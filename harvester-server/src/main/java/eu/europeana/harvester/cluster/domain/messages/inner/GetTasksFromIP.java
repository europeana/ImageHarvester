package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetTasksFromIP implements Serializable {

    private final String IP;

    public GetTasksFromIP(String ip) {
        IP = ip;
    }

    public String getIP() {
        return IP;
    }
}
