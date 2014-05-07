package eu.europeana.harvester.httpclient;

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
     * Whether to handle chunks or not. Always true.
     */
    private final Boolean handleChunks;

    public HttpRetrieveConfig(Duration limitsCheckInterval, Long bandwidthLimitWriteInBytesPerSec,
                              Long bandwidthLimitReadInBytesPerSec, Duration terminationThresholdTimeLimit,
                              Long terminationThresholdSizeLimitInBytes, Boolean handleChunks) {
        this.limitsCheckInterval = limitsCheckInterval;
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.terminationThresholdTimeLimit = terminationThresholdTimeLimit;
        this.terminationThresholdSizeLimitInBytes = terminationThresholdSizeLimitInBytes;
        this.handleChunks = handleChunks;
    }

    public HttpRetrieveConfig(Duration limitsCheckInterval, Long bandwidthLimitWriteInBytesPerSec,
                              Long bandwidthLimitReadInBytesPerSec) {
        this.limitsCheckInterval = limitsCheckInterval;
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.terminationThresholdTimeLimit = Duration.ZERO; /* no time limit */
        this.terminationThresholdSizeLimitInBytes = 0l; /* no content size limit */
        this.handleChunks = true;
    }

    public HttpRetrieveConfig() {
        this.limitsCheckInterval = Duration.millis(100);
        this.bandwidthLimitWriteInBytesPerSec = 0l; /* no bandwidth write limit */
        this.bandwidthLimitReadInBytesPerSec = 0l; /* no bandwidth read limit */
        this.terminationThresholdTimeLimit = Duration.ZERO; /* no time limit */
        this.terminationThresholdSizeLimitInBytes = 0l; /* no content size limit */
        this.handleChunks = true;
    }

    public Duration getLimitsCheckInterval() {
        return limitsCheckInterval;
    }

    public Long getBandwidthLimitWriteInBytesPerSec() {
        return bandwidthLimitWriteInBytesPerSec;
    }

    public Long getBandwidthLimitReadInBytesPerSec() {
        return bandwidthLimitReadInBytesPerSec;
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
        return "ReadLimit: " + getBandwidthLimitReadInBytesPerSec()/1024 + " kb/s" +
               "\nWriteLimit: " + getBandwidthLimitWriteInBytesPerSec()/1024 + " kb/s" +
               "\nCheckInterval: " + getLimitsCheckInterval() +
               "\nStop after: " + getTerminationThresholdSizeLimitInBytes() + " bytes" +
               " or after: " + getTerminationThresholdTimeLimit();
    }
}