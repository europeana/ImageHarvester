package eu.europeana.harvester.cluster.domain;

import org.joda.time.Duration;

/**
 * Stores various configuration arguments that the cluster master actor needs.
 */
public class ClusterMasterConfig {

    private final Duration jobsPollingInterval;

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
