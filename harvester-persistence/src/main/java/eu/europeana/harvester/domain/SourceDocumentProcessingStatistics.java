package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.Date;
import java.util.List;

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

    /**
     * The cause of the error if the task failed.
     */
    private final String log;

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
        this.log = null;
    }

    public SourceDocumentProcessingStatistics(final Date createdAt, final Date updatedAt, final ProcessingState state,
                                              final ReferenceOwner referenceOwner,
                                              final String sourceDocumentReferenceId,
                                              final String processingJobId, final Integer httpResponseCode,
                                              final String httpResponseContentType,
                                              final Long httpResponseContentSizeInBytes,
                                              final Long socketConnectToDownloadStartDurationInMilliSecs,
                                              final Long retrievalDurationInMilliSecs,
                                              final Long checkingDurationInMilliSecs,
                                              final String sourceIp, final List<ResponseHeader> httpResponseHeaders,
                                              final String log) {
        this.id = sourceDocumentReferenceId + "-" + processingJobId;
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
        this.log = log;
    }

    public SourceDocumentProcessingStatistics(final String id, final Date createdAt, final Date updatedAt,
                                              final ProcessingState state, final ReferenceOwner referenceOwner,
                                              final String sourceDocumentReferenceId, final String processingJobId,
                                              final Integer httpResponseCode, final String httpResponseContentType,
                                              final Long httpResponseContentSizeInBytes,
                                              final Long socketConnectToDownloadStartDurationInMilliSecs,
                                              final Long retrievalDurationInMilliSecs,
                                              final Long checkingDurationInMilliSecs,
                                              final String sourceIp, final List<ResponseHeader> httpResponseHeaders,
                                              final String log) {
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
        this.log = log;
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

    public String getLog() {
        return log;
    }

    public SourceDocumentProcessingStatistics withUpdate(final ProcessingState state,
                                                         final String jobId, final Integer responseCode,
                                                         final Long size,
                                                         final Long socketConnectToDownloadStartDurationInMilliSecs,
                                                         final Long retrievalDurationInMilliSecs,
                                                         final Long checkingDurationInMilliSecs,
                                                         final List<ResponseHeader> httpResponseHeaders,
                                                         final String log) {
        return new SourceDocumentProcessingStatistics(this.id, this.createdAt, new Date(), state, this.referenceOwner,
                this.sourceDocumentReferenceId, jobId, responseCode, this.httpResponseContentType, size,
                socketConnectToDownloadStartDurationInMilliSecs, retrievalDurationInMilliSecs,
                checkingDurationInMilliSecs, this.sourceIp, httpResponseHeaders, log);
    }

    public SourceDocumentProcessingStatistics withState(final ProcessingState state) {
        return new SourceDocumentProcessingStatistics(this.id, this.createdAt, new Date(), state, this.referenceOwner,
                this.sourceDocumentReferenceId, this.processingJobId, this.httpResponseCode,
                this.httpResponseContentType, this.httpResponseContentSizeInBytes,
                this.socketConnectToDownloadStartDurationInMilliSecs, this.retrievalDurationInMilliSecs,
                this.checkingDurationInMilliSecs, this.sourceIp, this.httpResponseHeaders, this.log);
    }

}
