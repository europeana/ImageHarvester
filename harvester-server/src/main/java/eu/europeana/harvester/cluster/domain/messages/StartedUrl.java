package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Message sent by slaves to notice the master actor that the download has started.
 */
public class StartedUrl implements Serializable {

    /**
     * The unique job ID.
     */
    private final String jobId;

    /**
     * The unique ID of the source document.
     */
    private final String sourceDocId;

    public StartedUrl(final String jobId, final String sourceDocId) {
        this.jobId = jobId;
        this.sourceDocId = sourceDocId;
    }

    public String getSourceDocId() {
        return sourceDocId;
    }

    public String getJobId() {
        return jobId;
    }
}
