package eu.europeana.harvester.cluster.messages;

import java.io.Serializable;

public class StartedUrl implements Serializable {

    private final String url;

    public StartedUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }
}
