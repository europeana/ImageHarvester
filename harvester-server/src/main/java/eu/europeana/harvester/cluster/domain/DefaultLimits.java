package eu.europeana.harvester.cluster.domain;

import java.io.Serializable;

public class DefaultLimits implements Serializable {

    /**
     * The number of task sent to a slave at a time.
     */
    private final Integer taskBatchSize;

    /**
     * The default limit on download speed if it has not been provided.
     */
    private final Long defaultBandwidthLimitReadInBytesPerSec;

    /**
     * The default limit on concurrent number of downloads if it has not been provided.
     */
    private final Long defaultMaxConcurrentConnectionsLimit;

    /**
     * The minimum interval which must elapse between two requests.
     */
    private final Integer minDistanceInMillisBetweenTwoRequest;

    /**
     * An int that specifies the connect timeout value in milliseconds.
     */
    private final Integer connectionTimeoutInMillis;

    /**
     * The number of redirects after which we stop the task.
     */
    private final Integer maxNrOfRedirects;

    /**
     * The minimum percentage of IPs which has loaded tasks in the memory.
     */
    private final Double minTasksPerIPPercentage;

    public DefaultLimits(final Integer taskBatchSize, final Long defaultBandwidthLimitReadInBytesPerSec,
                         final Long defaultMaxConcurrentConnectionsLimit,
                         Integer minDistanceInMillisBetweenTwoRequest, final Integer connectionTimeoutInMillis,
                         final Integer maxNrOfRedirects, final Double minTasksPerIPPercentage) {
        this.taskBatchSize = taskBatchSize;
        this.defaultBandwidthLimitReadInBytesPerSec = defaultBandwidthLimitReadInBytesPerSec;
        this.defaultMaxConcurrentConnectionsLimit = defaultMaxConcurrentConnectionsLimit;
        this.minDistanceInMillisBetweenTwoRequest = minDistanceInMillisBetweenTwoRequest;
        this.connectionTimeoutInMillis = connectionTimeoutInMillis;
        this.maxNrOfRedirects = maxNrOfRedirects;
        this.minTasksPerIPPercentage = minTasksPerIPPercentage;
    }

    public Long getDefaultBandwidthLimitReadInBytesPerSec() {
        return defaultBandwidthLimitReadInBytesPerSec;
    }

    public Long getDefaultMaxConcurrentConnectionsLimit() {
        return defaultMaxConcurrentConnectionsLimit;
    }

    public Integer getConnectionTimeoutInMillis() {
        return connectionTimeoutInMillis;
    }

    public Integer getMaxNrOfRedirects() {
        return maxNrOfRedirects;
    }

    public Integer getMinDistanceInMillisBetweenTwoRequest() {
        return minDistanceInMillisBetweenTwoRequest;
    }

    public Integer getTaskBatchSize() {
        return taskBatchSize;
    }

    public Double getMinTasksPerIPPercentage() {
        return minTasksPerIPPercentage;
    }
}
