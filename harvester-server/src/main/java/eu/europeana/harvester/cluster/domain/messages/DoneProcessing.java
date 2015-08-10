package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Message sent by slave actor and node master actor after the download is done.
 */
public class DoneProcessing implements Serializable {

    public final static JobState convertProcessingStateToJobState(final ProcessingState processingState) {
        switch (processingState) {
            case READY:
                return JobState.READY;
            case PAUSED:
                return JobState.PAUSED;
            case DOWNLOADING:
                return JobState.RUNNING;
            case SUCCESS:
                return JobState.FINISHED;
            case ERROR:
                return JobState.ERROR;
            case FAILED:
                return JobState.FAILED;
            default:
                return JobState.ERROR;
        }

    }

    public static ProcessingState getSubTaskAwareProcessingState(final ProcessingJobSubTaskStats subTaskStats) {
        switch (subTaskStats.getOverallState()) {
            case ERROR:
                return ProcessingState.ERROR;
            case FAILED:
                return ProcessingState.FAILED;
            case SUCCESS:
                return ProcessingState.SUCCESS;
            case NEVER_EXECUTED:
                /* I cannot imagine when this can happen unless something bad happened */
                return ProcessingState.ERROR;
            default:
                /* This should never happen */
                return ProcessingState.ERROR;
        }
    }

    /**
     * The ID of the task.
     */
    private final String taskID;

    /**
     * The url.
     */
    private final String url;

    /**
     * SourceDocumentReferenceId
     */
    private final String referenceId;

    /**
     * The caller jobs id.
     */
    private final String jobId;

    /**
     * The type of job which generates this document.
     */
    private final DocumentReferenceTaskType taskType;

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
    private final Map<String, String> httpResponseHeaders;

    /**
     * List of urls until we reach the final url.
     */
    private final List<String> redirectionPath;

    /**
     * Stores meta information about an image type document (width, height, mimetype, fileformat, colorspace,
     * filesize, colorPalette, orientation).
     */
    private final ImageMetaInfo imageMetaInfo;

    /**
     * Stores meta information about an audio type document (samplerate, bitrate, duration, fileformat,
     * mimetype, filesize, channels, bitdepth).
     */
    private final AudioMetaInfo audioMetaInfo;

    /**
     * Stores meta information about a video type document (width, height, duration,
     * framerate, mimetype, filesize, codec, resolution).
     */
    private final VideoMetaInfo videoMetaInfo;

    /**
     * Stores meta information about a text type document (mimetype, filesize, resolution).
     */
    private final TextMetaInfo textMetaInfo;

    private final ProcessingJobSubTaskStats stats;

    /**
     * The error message if there was any error.
     */
    private final String log;

    public DoneProcessing(
            final String taskID, final String url, String referenceId, final String jobId,
            final DocumentReferenceTaskType taskType,
            final HttpRetrieveResponse httpRetrieveResponse,
            final ProcessingJobSubTaskStats subTaskState,
            final ImageMetaInfo imageMetaInfo,
            final AudioMetaInfo audioMetaInfo, final VideoMetaInfo videoMetaInfo,
            final TextMetaInfo textMetaInfo, final String log) {
        this.taskType = taskType;
        this.taskID = taskID;
        this.url = url;
        this.referenceId = referenceId;
        this.jobId = jobId;
        this.httpResponseCode = (httpRetrieveResponse != null) ? httpRetrieveResponse.getHttpResponseCode() : -1;
        this.httpResponseContentType = (httpRetrieveResponse != null) ? httpRetrieveResponse.getHttpResponseContentType() : null;
        this.httpResponseContentSizeInBytes = (httpRetrieveResponse != null) ? httpRetrieveResponse.getContentSizeInBytes() : -1;
        this.socketConnectToDownloadStartDurationInMilliSecs =
                (httpRetrieveResponse != null) ? httpRetrieveResponse.getSocketConnectToDownloadStartDurationInMilliSecs() : -1;
        this.retrievalDurationInMilliSecs = (httpRetrieveResponse != null) ? httpRetrieveResponse.getRetrievalDurationInMilliSecs() : -1;
        this.checkingDurationInMilliSecs = (httpRetrieveResponse != null) ? httpRetrieveResponse.getCheckingDurationInMilliSecs() : -1;
        this.sourceIp = (httpRetrieveResponse != null) ? httpRetrieveResponse.getSourceIp() : null;
        this.httpResponseHeaders = (httpRetrieveResponse != null) ? httpRetrieveResponse.getResponseHeaders() : null;
        this.redirectionPath = (httpRetrieveResponse != null) ? httpRetrieveResponse.getRedirectionPath() : null;
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
        this.textMetaInfo = textMetaInfo;
        this.stats = subTaskState;
        this.log = log;
    }


    public DoneProcessing(final String taskID, final String url, String referenceId, final String jobId,
                          final DocumentReferenceTaskType taskType, final Integer httpResponseCode,
                          final String httpResponseContentType, final Long httpResponseContentSizeInBytes,
                          final Long socketConnectToDownloadStartDurationInMilliSecs,
                          final Long retrievalDurationInMilliSecs, final Long checkingDurationInMilliSecs,
                          final String sourceIp, final Map<String, String> httpResponseHeaders,
                          final List<String> redirectionPath,
                          final ProcessingJobSubTaskStats stats, final String log,
                          final ImageMetaInfo imageMetaInfo, final AudioMetaInfo audioMetaInfo,
                          final VideoMetaInfo videoMetaInfo, final TextMetaInfo textMetaInfo) {
        this.taskID = taskID;
        this.url = url;
        this.referenceId = referenceId;
        this.jobId = jobId;
        this.taskType = taskType;
        this.httpResponseCode = httpResponseCode;
        this.httpResponseContentType = httpResponseContentType;
        this.httpResponseContentSizeInBytes = httpResponseContentSizeInBytes;
        this.socketConnectToDownloadStartDurationInMilliSecs = socketConnectToDownloadStartDurationInMilliSecs;
        this.retrievalDurationInMilliSecs = retrievalDurationInMilliSecs;
        this.checkingDurationInMilliSecs = checkingDurationInMilliSecs;
        this.sourceIp = sourceIp;
        this.httpResponseHeaders = httpResponseHeaders;
        this.redirectionPath = redirectionPath;
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
        this.textMetaInfo = textMetaInfo;
        this.stats = stats;
        this.log = log;
    }

    public String getUrl() {
        return url;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public String getJobId() {
        return jobId;
    }

    public DocumentReferenceTaskType getTaskType() {
        return taskType;
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

    public Map<String, String> getHttpResponseHeaders() {
        return httpResponseHeaders;
    }

    public Long getSocketConnectToDownloadStartDurationInMilliSecs() {
        return socketConnectToDownloadStartDurationInMilliSecs;
    }

    public List<String> getRedirectionPath() {
        return redirectionPath;
    }

    public ImageMetaInfo getImageMetaInfo() {
        return imageMetaInfo;
    }

    public AudioMetaInfo getAudioMetaInfo() {
        return audioMetaInfo;
    }

    public VideoMetaInfo getVideoMetaInfo() {
        return videoMetaInfo;
    }

    public TextMetaInfo getTextMetaInfo() {
        return textMetaInfo;
    }

    public ProcessingJobSubTaskStats getStats() {
        return stats;
    }

    public ProcessingState getProcessingState() {
        return getSubTaskAwareProcessingState(this.stats);
    }

    public String getLog() {
        return log;
    }

    public String getTaskID() {
        return taskID;
    }

    public DoneProcessing withProcessingInfo(final ProcessingJobSubTaskStats newSubTaskState,
                                             final ImageMetaInfo newImageMetaInfo,
                                             final AudioMetaInfo newAudioMetaInfo, final VideoMetaInfo newVideoMetaInfo,
                                             final TextMetaInfo newTextMetaInfo) {
        return new DoneProcessing(taskID, url, referenceId, jobId,
                taskType, httpResponseCode,
                httpResponseContentType, httpResponseContentSizeInBytes,
                socketConnectToDownloadStartDurationInMilliSecs,
                retrievalDurationInMilliSecs, checkingDurationInMilliSecs,
                sourceIp, httpResponseHeaders,
                redirectionPath,
                newSubTaskState, log,
                newImageMetaInfo, newAudioMetaInfo,
                newVideoMetaInfo, newTextMetaInfo);
    }
}
