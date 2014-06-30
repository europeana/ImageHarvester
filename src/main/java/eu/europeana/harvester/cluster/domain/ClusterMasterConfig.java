package eu.europeana.harvester.cluster.domain;

import com.mongodb.WriteConcern;
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

    /**
     * Describes the guarantee that MongoDB provides when reporting on the success of a write operation
     */
    private final WriteConcern writeConcern;

    public ClusterMasterConfig(Duration jobsPollingInterval, Duration receiveTimeoutInterval, WriteConcern writeConcern) {
        this.jobsPollingInterval = jobsPollingInterval;
        this.receiveTimeoutInterval = receiveTimeoutInterval;
        this.writeConcern = writeConcern;
    }

    public Duration getJobsPollingInterval() {
        return jobsPollingInterval;
    }

    public Duration getReceiveTimeoutInterval() {
        return receiveTimeoutInterval;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
