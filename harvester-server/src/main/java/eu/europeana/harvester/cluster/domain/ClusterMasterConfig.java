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
     * The time interval in milliseconds between each iteration of sending new jobs.
     */
    private final Duration taskStartingInterval;

    /**
     * The maximum number of jobs that you can retrieve from the db at a time.
     */
    private final Integer maxJobsPerIteration;

    /**
     * The time interval in milliseconds after that akka actor gives timeout error.
     */
    private final Duration receiveTimeoutInterval;

    /**
     * The time interval in milliseconds after that a sent task will be reinitialized
     * if there is no response from any slave
     */
    private final Integer responseTimeoutFromSlaveInMillis;

    /**
     * Describes the guarantee that MongoDB provides when reporting on the success of a write operation
     */
    private final WriteConcern writeConcern;

    public ClusterMasterConfig(final Duration jobsPollingInterval, final Duration taskStartingInterval,
                               final Integer maxJobsPerIteration, final Duration receiveTimeoutInterval,
                               final Integer responseTimeoutFromSlaveInMillis, final WriteConcern writeConcern) {
        this.jobsPollingInterval = jobsPollingInterval;
        this.taskStartingInterval = taskStartingInterval;
        this.maxJobsPerIteration = maxJobsPerIteration;
        this.receiveTimeoutInterval = receiveTimeoutInterval;
        this.responseTimeoutFromSlaveInMillis = responseTimeoutFromSlaveInMillis;
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

    public Integer getMaxJobsPerIteration() {
        return maxJobsPerIteration;
    }

    public Duration getTaskStartingInterval() {
        return taskStartingInterval;
    }

    public Integer getResponseTimeoutFromSlaveInMillis() {
        return responseTimeoutFromSlaveInMillis;
    }
}
