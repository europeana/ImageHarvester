package eu.europeana.harvester.cluster.domain.messages;

public class RetrieveUrlWithProcessingConfig {
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
