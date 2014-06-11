package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Message sent by slaves to notice the master actor that the download has started.
 */
public class StartedUrl implements Serializable {

    /**
     * The url.
     */
    private final String url;

    public StartedUrl(String url) {
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

}
