package eu.europeana.harvester.httpclient.response;

/**
 * The response state.
 */
public enum ResponseState {
    ERROR,
    COMPLETED,
    PREPARING,
    PROCESSING,
    FINISHED_TIME_LIMIT,
    FINISHED_SIZE_LIMIT
}
