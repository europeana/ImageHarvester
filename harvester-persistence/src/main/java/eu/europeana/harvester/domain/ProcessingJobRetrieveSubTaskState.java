package eu.europeana.harvester.domain;

public enum ProcessingJobRetrieveSubTaskState {
    SUCCESS,
    ERROR,
    FAILED,
    FINISHED_RATE_LIMIT,
    FINISHED_TIME_LIMIT,
    FINISHED_SIZE_LIMIT,
    NEVER_EXECUTED
}

