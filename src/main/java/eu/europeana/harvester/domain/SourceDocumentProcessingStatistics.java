package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import eu.europeana.harvester.httpclient.response.ResponseHeader;

import java.util.*;

/**
 * Stores the stats for a specific processing job for a source.
 */
public class SourceDocumentProcessingStatistics {

    @Id
    @Property("id")
	private final String id;

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
     * An object which contains: provider id, collection id, record id
     */
    private final ReferenceOwner referenceOwner;

    /**
     * The reference to the source document.
     */
	private final String sourceDocumentReferenceId;

    /**
     * The processing job that executed.
     */
	private final String processingJobId;

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
     * The duration in milliseconds between the socket connection and the first content bytes coming in.
     * This is relevant as it indicates the amount of time the server spends to simply make the resource
     * available. For example a resource coming from a CDN will have this very low and one coming from a
     * slow database will be rather large. Zero if the source is not retrieved.
     */
    private final Long socketConnectToDownloadStartDurationInMilliSecs;

    /**
     * The retrieval duration in milliseconds. Zero if the source is not retrieved.
     */
    private final Long retrievalDurationInMilliSecs;

    /**
     * The checking duration in milliseconds. The same as the retrieval if the source is retrieved.
     */
    private final Long checkingDurationInMilliSecs;

    /**
     * The IP of the source. Useful for debugging when working with DNS load balanced sources that have a pool of real
     * IP's for the same domain name.
     */
	private final String sourceIp;

    /**
     * The HTTP response headers.
     */
	private final List<ResponseHeader> httpResponseHeaders;

    public SourceDocumentProcessingStatistics() {
        this.id = null;
        this.createdAt = null;
        this.updatedAt = null;
        this.state = null;
        this.referenceOwner = null;
        this.sourceDocumentReferenceId = null;
        this.processingJobId = null;
        this.httpResponseCode = null;
        this.httpResponseContentType = null;
        this.httpResponseContentSizeInBytes = null;
        this.socketConnectToDownloadStartDurationInMilliSecs = null;
        this.retrievalDurationInMilliSecs = null;
        this.checkingDurationInMilliSecs = null;
        this.sourceIp = null;
        this.httpResponseHeaders = null;
    }

    public SourceDocumentProcessingStatistics(Date createdAt,Date updatedAt, ProcessingState state,
                                              ReferenceOwner referenceOwner, String sourceDocumentReferenceId,
                                              String processingJobId, Integer httpResponseCode,
                                              String httpResponseContentType, Long httpResponseContentSizeInBytes,
                                              Long socketConnectToDownloadStartDurationInMilliSecs,
                                              Long retrievalDurationInMilliSecs, Long checkingDurationInMilliSecs,
                                              String sourceIp, List<ResponseHeader> httpResponseHeaders) {
        this.id = UUID.randomUUID().toString();
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.processingJobId = processingJobId;
        this.httpResponseCode = httpResponseCode;
        this.httpResponseContentType = httpResponseContentType;
        this.httpResponseContentSizeInBytes = httpResponseContentSizeInBytes;
        this.socketConnectToDownloadStartDurationInMilliSecs = socketConnectToDownloadStartDurationInMilliSecs;
        this.retrievalDurationInMilliSecs = retrievalDurationInMilliSecs;
        this.checkingDurationInMilliSecs = checkingDurationInMilliSecs;
        this.sourceIp = sourceIp;
        this.httpResponseHeaders = httpResponseHeaders;
    }

    public SourceDocumentProcessingStatistics(String id, Date createdAt,Date updatedAt, ProcessingState state,
                                              ReferenceOwner referenceOwner,
                                              String sourceDocumentReferenceId, String processingJobId,
                                              Integer httpResponseCode, String httpResponseContentType,
                                              Long httpResponseContentSizeInBytes,
                                              Long socketConnectToDownloadStartDurationInMilliSecs,
                                              Long retrievalDurationInMilliSecs, Long checkingDurationInMilliSecs,
                                              String sourceIp, List<ResponseHeader> httpResponseHeaders) {
        this.id = id;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.state = state;
        this.referenceOwner = referenceOwner;
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.processingJobId = processingJobId;
        this.httpResponseCode = httpResponseCode;
        this.httpResponseContentType = httpResponseContentType;
        this.httpResponseContentSizeInBytes = httpResponseContentSizeInBytes;
        this.socketConnectToDownloadStartDurationInMilliSecs = socketConnectToDownloadStartDurationInMilliSecs;
        this.retrievalDurationInMilliSecs = retrievalDurationInMilliSecs;
        this.checkingDurationInMilliSecs = checkingDurationInMilliSecs;
        this.sourceIp = sourceIp;
        this.httpResponseHeaders = httpResponseHeaders;
    }


    public String getId() {
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

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public String getSourceDocumentReferenceId() {
        return sourceDocumentReferenceId;
    }

    public String getProcessingJobId() {
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

    public Long getRetrievalDurationInMilliSecs() {
        return retrievalDurationInMilliSecs;
    }

    public Long getCheckingDurationInMilliSecs() {
        return checkingDurationInMilliSecs;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public List<ResponseHeader> getHttpResponseHeaders() {
        return httpResponseHeaders;
    }

    public Long getSocketConnectToDownloadStartDurationInMilliSecs() {
        return socketConnectToDownloadStartDurationInMilliSecs;
    }

    public SourceDocumentProcessingStatistics withUpdate(Date updatedAt, ProcessingState state, String jobId,
                                                         Integer responseCode, Long size,
                                                         Long socketConnectToDownloadStartDurationInMilliSecs,
                                                         Long retrievalDurationInMilliSecs,
                                                         Long checkingDurationInMilliSecs,
                                                         List<ResponseHeader> httpResponseHeaders) {
        return new SourceDocumentProcessingStatistics(this.id, this.createdAt, updatedAt, state, this.referenceOwner,
                this.sourceDocumentReferenceId, jobId, responseCode, this.httpResponseContentType, size,
                socketConnectToDownloadStartDurationInMilliSecs, retrievalDurationInMilliSecs,
                checkingDurationInMilliSecs, this.sourceIp, httpResponseHeaders);
    }

}
