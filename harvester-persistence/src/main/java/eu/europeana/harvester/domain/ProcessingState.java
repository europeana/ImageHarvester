package eu.europeana.harvester.domain;

/**
 * The possible states of a task.
 */
public enum ProcessingState {
    /**
     * Not started but ready for execution.
     */
    READY,
    /**
     * A task from a paused job.
     */
    PAUSED,
    /**
     * Started task.
     */
    DOWNLOADING,
    /**
     * Successfully finished task.
     */
    SUCCESS,
    /**
     * Error task.
     */
    ERROR,
    /**
     * Failed task.
     */
    FAILED

}

