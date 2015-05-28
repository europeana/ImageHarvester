package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import org.joda.time.Duration;

import java.io.Serializable;

/**
 * The HTTP retrieve operation settings that control the bandwidth allocation & time/size limits for a request.
 * Can & should be reused across multiple requests.
 */
public class HttpRetrieveConfig implements Serializable {

    /**
     * The interval at which the "time wheel" checks whether the limits have been reached. Must be > 0.
     */
    private final Duration limitsCheckInterval;

    /**
     * The bandwidth limit usage for write (ie. sending). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitWriteInBytesPerSec;

    /**
     * The bandwidth limit usage for read (ie. receiving). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitReadInBytesPerSec;

    /**
     * The time threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Duration terminationThresholdTimeLimit;

    /**
     * The content size threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Long terminationThresholdSizeLimitInBytes;


    /**
     * The threshold for the download rate after which the retrieval is terminated.
     */
    private final Long terminationThresholdReadPerSecondInBytes;

    /**
     * Whether to handle chunks or not. Always true.
     */
    private final Boolean handleChunks;

    /**
     * The specific task type: check limit, conditional or unconditional download.
     */
    private final DocumentReferenceTaskType taskType;

    /**
     * An int that specifies the connect timeout value in milliseconds.
     */
    private final Integer connectionTimeoutInMillis;

    /**
     * The number of redirects after which we stop the task.
     */
    private final Integer maxNrOfRedirects;

    public HttpRetrieveConfig(final Duration limitsCheckInterval, final Long bandwidthLimitWriteInBytesPerSec,
                              final Long bandwidthLimitReadInBytesPerSec, final Duration terminationThresholdTimeLimit,
                              final Long terminationThresholdSizeLimitInBytes,
                              final Long terminationThresholdReadPerSecondInBytes,
                              final Boolean handleChunks,
                              final DocumentReferenceTaskType taskType, final Integer connectionTimeoutInMillis,
                              final Integer maxNrOfRedirects) {
        this.limitsCheckInterval = limitsCheckInterval;
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.terminationThresholdTimeLimit = terminationThresholdTimeLimit;
        this.terminationThresholdSizeLimitInBytes = terminationThresholdSizeLimitInBytes;
        this.terminationThresholdReadPerSecondInBytes = terminationThresholdReadPerSecondInBytes;
        this.handleChunks = handleChunks;
        this.taskType = taskType;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
        this.maxNrOfRedirects = maxNrOfRedirects;
    }

    public HttpRetrieveConfig(final Duration limitsCheckInterval, final Long bandwidthLimitWriteInBytesPerSec,
                              final Long bandwidthLimitReadInBytesPerSec,
                              final Long terminationThresholdReadPerSecondInBytes,
                              final Duration terminationThresholdTimeLimit,
                              final DocumentReferenceTaskType taskType,
                              final Integer connectionTimeoutInMillis, final Integer maxNrOfRedirects) {
        this.limitsCheckInterval = limitsCheckInterval;
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.terminationThresholdTimeLimit = terminationThresholdTimeLimit;
        this.taskType = taskType;
        this.maxNrOfRedirects = maxNrOfRedirects;
        this.terminationThresholdSizeLimitInBytes = 0l; /* no content size limit */
        this.terminationThresholdReadPerSecondInBytes = terminationThresholdReadPerSecondInBytes;
        this.handleChunks = true;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
    }

    public HttpRetrieveConfig() {
        this.limitsCheckInterval = Duration.standardSeconds(10);
        this.bandwidthLimitWriteInBytesPerSec = 0l; /* no bandwidth write limit */
        this.bandwidthLimitReadInBytesPerSec = 0l; /* no bandwidth read limit */
        this.terminationThresholdTimeLimit = Duration.standardMinutes(30);
        this.terminationThresholdSizeLimitInBytes = 0l; /* no content size limit */
        this.terminationThresholdReadPerSecondInBytes = 5*1000l;
        this.handleChunks = true;
        this.taskType = null;
        this.connectionTimeoutInMillis = 10000; /* 10 seconds connection timeout limit*/
        this.maxNrOfRedirects = 10;
    }

    public Duration getLimitsCheckInterval() {
        return limitsCheckInterval;
    }

    public Long getBandwidthLimitWriteInBytesPerSec() {
        return 0L; /*bandwidthLimitWriteInBytesPerSec;*/
    }

    public Long getBandwidthLimitReadInBytesPerSec() {
        return 0L; /*bandwidthLimitReadInBytesPerSec;*/
    }

    public Duration getTerminationThresholdTimeLimit() {
        return terminationThresholdTimeLimit;
    }

    public Long getTerminationThresholdSizeLimitInBytes() {
        return terminationThresholdSizeLimitInBytes;
    }

    public Boolean getHandleChunks() {
        return handleChunks;
    }

    @Override
    public String toString() {
        return "ReadLimit: " + bandwidthLimitReadInBytesPerSec/1024 + " kb/s" +
               "\nWriteLimit: " + bandwidthLimitWriteInBytesPerSec/1024 + " kb/s" +
               "\nCheckInterval: " + limitsCheckInterval +
               "\nStop after: " + terminationThresholdSizeLimitInBytes + " bytes" +
               " or after: " + terminationThresholdTimeLimit + "." +
                "\nMax number of redirects: " + maxNrOfRedirects;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
    }

    public Integer getConnectionTimeoutInMillis() {
        return connectionTimeoutInMillis;
    }

    public Long getTerminationThresholdReadPerSecondInBytes() {
        return terminationThresholdReadPerSecondInBytes;
    }

    public Integer getMaxNrOfRedirects() {
        return maxNrOfRedirects;
    }

}
