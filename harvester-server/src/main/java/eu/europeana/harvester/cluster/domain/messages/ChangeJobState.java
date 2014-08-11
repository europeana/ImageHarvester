package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.JobState;

import java.io.Serializable;

/**
 * Sent by cluster master to all node masters when it notice a change in the state of a job (if it was paused or resumed)
 */
public class ChangeJobState implements Serializable {

    /**
     * The new state of the job.
     */
    private final JobState newState;

    /**
     * The unique id of the job.
     */
    private final String jobId;

    public ChangeJobState(final JobState newState, final String jobId) {
        this.newState = newState;
        this.jobId = jobId;
    }

    public JobState getNewState() {
        return newState;
    }

    public String getJobId() {
        return jobId;
    }
}
