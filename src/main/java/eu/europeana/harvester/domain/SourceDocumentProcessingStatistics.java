package eu.europeana.harvester.domain;

import java.util.ArrayList;
import java.util.Date;

/**
 * Stores the stats for a specific processing job for a source.
 */
public class SourceDocumentProcessingStatistics {
	private final Long id;

    /**
     * When was the statistic created.
     */
    private final Date createdAt;

    /**
     * When was the statistic last updated.
     */
    private final Date updatedAt;

    /**
     * The processing state.
     */
	private final ProcessingState state;

    /**
     * The provider that owns the source document.
     */
	private final Long providerId;

    /**
     * The collection that owns the source document.
     */
	private final Long collectionId;

    /**
     * The record that owns the source document.
     */
	private final Long recordId;

    /**
     * The reference to the source document.
     */
	private final Long sourceDocumentReferenceId;

    /**
     * The processing job that executed.
     */
	private final Long processingJobId;

    /**
     * The HTTP response code.
     */
	private final Integer httpResponseCode;

    /**
     * The HTTP response content type.
     */
    private final String httpResponseContentType;

    /**
     * The HTTP response content size in bytes.
     */
    private final Long httpResponseContentSizeInBytes;

    /**
     * The retrieval duration in seconds. Zero if the source is not retrieved.
     */
    private final Long retrievalDurationInSecs;

    /**
     * The checking duration in seconds. The same as the retrieval if the source is retrieved.
     */
    private final Long checkingDurationInSecs;

    /**
     * The IP of the source. Useful for debugging when working with DNS load balanced sources that have a pool of real
     * IP's for the same domain name.
     */
	private final String sourceIp;

    /**
     * The HTTP response headers.
     */
	private final ArrayList<Byte> httpResponseHeaders;

    /**
	 * Logs. Useful for debugging purposes
	 */
	private final ArrayList<Byte> logs;

    public SourceDocumentProcessingStatistics(Long id, Date createdAt,Date updatedAt, ProcessingState state, Long providerId, Long collectionId, Long recordId, Long sourceDocumentReferenceId, Long processingJobId, Integer httpResponseCode, String httpResponseContentType, Long httpResponseContentSizeInBytes, Long retrievalDurationInSecs, Long checkingDurationInSecs, String sourceIp, ArrayList<Byte> httpResponseHeaders, ArrayList<Byte> logs) {
        this.id = id;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.providerId = providerId;
        this.collectionId = collectionId;
        this.recordId = recordId;
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.processingJobId = processingJobId;
        this.httpResponseCode = httpResponseCode;
        this.httpResponseContentType = httpResponseContentType;
        this.httpResponseContentSizeInBytes = httpResponseContentSizeInBytes;
        this.retrievalDurationInSecs = retrievalDurationInSecs;
        this.checkingDurationInSecs = checkingDurationInSecs;
        this.sourceIp = sourceIp;
        this.httpResponseHeaders = httpResponseHeaders;
        this.logs = logs;
    }

    public Long getId() {
        return id;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public ProcessingState getState() {
        return state;
    }

    public Long getProviderId() {
        return providerId;
    }

    public Long getCollectionId() {
        return collectionId;
    }

    public Long getRecordId() {
        return recordId;
    }

    public Long getSourceDocumentReferenceId() {
        return sourceDocumentReferenceId;
    }

    public Long getProcessingJobId() {
        return processingJobId;
    }

    public Integer getHttpResponseCode() {
        return httpResponseCode;
    }

    public String getHttpResponseContentType() {
        return httpResponseContentType;
    }

    public Long getHttpResponseContentSizeInBytes() {
        return httpResponseContentSizeInBytes;
    }

    public Long getRetrievalDurationInSecs() {
        return retrievalDurationInSecs;
    }

    public Long getCheckingDurationInSecs() {
        return checkingDurationInSecs;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public ArrayList<Byte> getHttpResponseHeaders() {
        return httpResponseHeaders;
    }

    public ArrayList<Byte> getLogs() {
        return logs;
    }
}
