package eu.europeana.harvester.cluster.messages;

import java.io.Serializable;
import java.net.URL;

public class DoneDownload implements Serializable {

    private final URL url;

    public DoneDownload(URL url) {
        this.url = url;
    }

    public URL getUrl() {
        return url;
    }
}
