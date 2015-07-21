package eu.europeana.harvester.cluster.domain;

import com.mongodb.WriteConcern;
import eu.europeana.harvester.cluster.master.jobrestarter.JobRestarterConfig;
import org.joda.time.Duration;

/**
 * Stores various configuration properties needed by the cluster master actor.
 */
public class ClusterMasterConfig {

    /**
     * The maximum number of jobs that you can retrieve from an IP.
     */
    private final Integer jobsPerIP;

    /**
     * The maximum number of tasks that can be kept in memory.
     */
    private final Long maxTasksInMemory;

    // TODO: remove
    /**
     * The time interval in milliseconds after that akka actor gives timeout error.
     */
    private final Duration receiveTimeoutInterval;

    /**
     * The time interval in milliseconds after that a sent task will be reinitialized
     * if there is no response from any slave
     */
    private final Integer responseTimeoutFromSlaveInMillis;

    private final JobRestarterConfig jobRestarterConfig;

    /**
     * Describes the guarantee that MongoDB provides when reporting on the success of a write operation
     */
    private final WriteConcern writeConcern;

    public ClusterMasterConfig (final Integer jobsPerIP, final Long maxTasksInMemory, final Duration receiveTimeoutInterval, final Integer responseTimeoutFromSlaveInMillis,
                                JobRestarterConfig jobRestarterConfig, final WriteConcern writeConcern) {
        this.jobsPerIP = jobsPerIP;
        this.maxTasksInMemory = maxTasksInMemory;
        this.receiveTimeoutInterval = receiveTimeoutInterval;
        this.responseTimeoutFromSlaveInMillis = responseTimeoutFromSlaveInMillis;
        this.jobRestarterConfig = jobRestarterConfig;
        this.writeConcern = writeConcern;
    }

    public Duration getReceiveTimeoutInterval() {
        return receiveTimeoutInterval;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }

    public Integer getJobsPerIP() {
        return jobsPerIP;
    }

    public Integer getResponseTimeoutFromSlaveInMillis() {
        return responseTimeoutFromSlaveInMillis;
    }

    public Long getMaxTasksInMemory() {
        return maxTasksInMemory;
    }

    public JobRestarterConfig getJobRestarterConfig () {
        return jobRestarterConfig;
    }
}
