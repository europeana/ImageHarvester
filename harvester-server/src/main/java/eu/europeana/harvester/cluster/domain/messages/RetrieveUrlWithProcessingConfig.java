package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

public class RetrieveUrlWithProcessingConfig implements Serializable {
    private final RetrieveUrl retrieveUrl;
    private final String downloadPath;
    private final String processingSource;

    public RetrieveUrlWithProcessingConfig(RetrieveUrl retrieveUrl, String downloadPath, String processingSource) {
        this.retrieveUrl = retrieveUrl;
        this.downloadPath = downloadPath;
        this.processingSource = processingSource;
    }

    public RetrieveUrl getRetrieveUrl() {
        return retrieveUrl;
    }

    public String getDownloadPath() {
        return downloadPath;
    }

    public String getProcessingSource() {
        return processingSource;
    }
}
