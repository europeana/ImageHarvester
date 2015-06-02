package eu.europeana.jobcreator.domain;

public class ProcessingJobCreationOptions {
    private final boolean forceUnconditionalDownload;

    public ProcessingJobCreationOptions(boolean forceUnconditionalDownload) {
        this.forceUnconditionalDownload = forceUnconditionalDownload;
    }

    public boolean isForceUnconditionalDownload() {
        return forceUnconditionalDownload;
    }
}