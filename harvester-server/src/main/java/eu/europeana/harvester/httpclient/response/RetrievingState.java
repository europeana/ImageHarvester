package eu.europeana.harvester.httpclient.response;

/**
 * The response state.
 */
public enum RetrievingState {
    ERROR,
    COMPLETED,
    PREPARING,
    PROCESSING,
    FINISHED_RATE_LIMIT,
    FINISHED_TIME_LIMIT,
    FINISHED_SIZE_LIMIT
}
