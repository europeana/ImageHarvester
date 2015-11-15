package eu.europeana.harvester.client.report;

/**
 * An aggregation of possible subtask states. Containes both the states for retrieval and also for processing.
 */
public enum SubTaskState {
    SUCCESS,
    ERROR,
    FAILED,
    FINISHED_RATE_LIMIT,
    FINISHED_TIME_LIMIT,
    FINISHED_SIZE_LIMIT,
    NEVER_EXECUTED
}
