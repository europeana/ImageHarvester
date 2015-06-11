package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

public class RetrieveUrlWithProcessingConfig implements Serializable {
    private final RetrieveUrl retrieveUrl;
    private final String downloadPath;

    public RetrieveUrlWithProcessingConfig(RetrieveUrl retrieveUrl, String downloadPath) {
        this.retrieveUrl = retrieveUrl;
        this.downloadPath = downloadPath;
    }

    public RetrieveUrl getRetrieveUrl() {
        return retrieveUrl;
    }

    public String getDownloadPath() {
        return downloadPath;
    }
}
