package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetNumberOfParallelDownloadsPerIP implements Serializable {

    private final String IP;

    public GetNumberOfParallelDownloadsPerIP(String ip) {
        IP = ip;
    }

    public String getIP() {
        return IP;
    }
}
