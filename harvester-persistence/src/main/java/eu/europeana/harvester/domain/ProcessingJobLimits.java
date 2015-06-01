package eu.europeana.harvester.domain;

public class ProcessingJobLimits {

    /**
     * The time threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Long retrievalTerminationThresholdTimeLimitInMillis;

    /**
     * The threshold for the download rate after which the retrieval is terminated.
     */
    private final Long retrievalTerminationThresholdReadPerSecondInBytes;

    /**
     * An int that specifies the connect timeout value in milliseconds.
     */
    private final Long retrievalConnectionTimeoutInMillis;

    /**
     * The number of redirects after which we stop the task.
     */
    private final Integer retrievalMaxNrOfRedirects;

    /**
     * The time threshold after which the processing is terminated. 0 means no limit.
     */
    private final Long processingTerminationThresholdTimeLimitInMillis;

    public ProcessingJobLimits() {
        this(
                30 * 60 * 1000l,  /* 30 MINUTES -> retrievalTerminationThresholdTimeLimitInMillis */
                5 * 1000l, /* 5 KB/s -> retrievalTerminationThresholdReadPerSecondInBytes */
                30 * 1000l, /* 30 SECONDS -> retrievalConnectionTimeoutInMillis */
                5, /* retrievalMaxNrOfRedirects */
                30 * 60 * 1000l  /* 30 MINUTES -> processingTerminationThresholdTimeLimitInMillis */

        );
    }

    public ProcessingJobLimits(Long retrievalTerminationThresholdTimeLimitInMillis, Long retrievalTerminationThresholdReadPerSecondInBytes, Long retrievalConnectionTimeoutInMillis, Integer retrievalMaxNrOfRedirects, Long processingTerminationThresholdTimeLimitInMillis) {
        this.retrievalTerminationThresholdTimeLimitInMillis = retrievalTerminationThresholdTimeLimitInMillis;
        this.retrievalTerminationThresholdReadPerSecondInBytes = retrievalTerminationThresholdReadPerSecondInBytes;
        this.retrievalConnectionTimeoutInMillis = retrievalConnectionTimeoutInMillis;
        this.retrievalMaxNrOfRedirects = retrievalMaxNrOfRedirects;
        this.processingTerminationThresholdTimeLimitInMillis = processingTerminationThresholdTimeLimitInMillis;
    }

    public Long getRetrievalTerminationThresholdTimeLimitInMillis() {
        return retrievalTerminationThresholdTimeLimitInMillis;
    }

    public Long getRetrievalTerminationThresholdReadPerSecondInBytes() {
        return retrievalTerminationThresholdReadPerSecondInBytes;
    }

    public Long getRetrievalConnectionTimeoutInMillis() {
        return retrievalConnectionTimeoutInMillis;
    }

    public Integer getRetrievalMaxNrOfRedirects() {
        return retrievalMaxNrOfRedirects;
    }

    public Long getProcessingTerminationThresholdTimeLimitInMillis() {
        return processingTerminationThresholdTimeLimitInMillis;
    }
}
