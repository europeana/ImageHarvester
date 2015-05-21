package eu.europeana.harvester.domain;

/**
 * An enum which contains all possible states of a processing job.
 */
public enum JobState {
    /**
     * Ready for processing but not started yet.
     */
    READY,
    /**
     * Loaded by the master
     */
    LOADED,
    /**
     * Started job.
     */
    RUNNING,
    /**
     * Finished job.
     */
    FINISHED,
    /**
     * Job with error
     */
    ERROR,
    /**
     * A job paused by client but not ready paused by application.
     */
    PAUSE,
    /**
     * A job paused by client and by application.
     */
    PAUSED,
    /**
     * A job which is ready to return to RUNNING state.
     */
    RESUME
}
