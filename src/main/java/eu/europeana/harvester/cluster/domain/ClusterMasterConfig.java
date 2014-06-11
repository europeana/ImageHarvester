package eu.europeana.harvester.cluster.domain;

import org.joda.time.Duration;

/**
 * Stores various configuration properties needed by the cluster master actor.
 */
public class ClusterMasterConfig {

    /**
     * The time interval in milliseconds between each searching for new jobs.
     */
    private final Duration jobsPollingInterval;

    /**
     * The time interval in milliseconds after that akka actor gives timeout error.
     */
    private final Duration receiveTimeoutInterval;

    public ClusterMasterConfig(Duration jobsPollingInterval, Duration receiveTimeoutInterval) {
        this.jobsPollingInterval = jobsPollingInterval;
        this.receiveTimeoutInterval = receiveTimeoutInterval;
    }

    public Duration getJobsPollingInterval() {
        return jobsPollingInterval;
    }

    public Duration getReceiveTimeoutInterval() {
        return receiveTimeoutInterval;
    }
}
