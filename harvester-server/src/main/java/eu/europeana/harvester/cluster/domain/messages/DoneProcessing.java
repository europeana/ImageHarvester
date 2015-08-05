package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.RetrievingState;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Message sent by slave actor and node master actor after the download is done.
 */
public class DoneProcessing implements Serializable {

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

    /**
     * The state of the task. This has wider scope than the state of the individual sub tasks because it also
     * covers all the rest of processing that goes outside the sub tasks.
     */
    private final ProcessingState processingState;

    private final RetrievingState retrievingState;

    private final ProcessingJobSubTaskStats stats;

    /**
     * The error message if there was any error.
     */
    private final String log;


    public DoneProcessing(final DoneDownload doneDownload,
                          final ProcessingJobSubTaskStats subTaskState,
                          final ImageMetaInfo imageMetaInfo,
                          final AudioMetaInfo audioMetaInfo, final VideoMetaInfo videoMetaInfo,
                          final TextMetaInfo textMetaInfo, final String log) {
        this.taskType = doneDownload.getDocumentReferenceTask().getTaskType();
        final HttpRetrieveResponse httpRetrieveResponse = doneDownload.getHttpRetrieveResponse();

        this.taskID = doneDownload.getTaskID();
        this.url = doneDownload.getUrl();
        this.referenceId = doneDownload.getReferenceId();
        this.jobId = doneDownload.getJobId();
        this.httpResponseCode = httpRetrieveResponse.getHttpResponseCode();
        this.httpResponseContentType = httpRetrieveResponse.getHttpResponseContentType();
        this.httpResponseContentSizeInBytes = httpRetrieveResponse.getContentSizeInBytes();
        this.socketConnectToDownloadStartDurationInMilliSecs =
                httpRetrieveResponse.getSocketConnectToDownloadStartDurationInMilliSecs();
        this.retrievalDurationInMilliSecs = httpRetrieveResponse.getRetrievalDurationInMilliSecs();
        this.checkingDurationInMilliSecs = httpRetrieveResponse.getCheckingDurationInMilliSecs();
        this.sourceIp = httpRetrieveResponse.getSourceIp();
        this.httpResponseHeaders = httpRetrieveResponse.getResponseHeaders();
        this.redirectionPath = httpRetrieveResponse.getRedirectionPath();
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
        this.textMetaInfo = textMetaInfo;

        if (null == subTaskState) {
            switch (doneDownload.getRetrieveState()) {
                case COMPLETED:
                    this.stats = new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.SUCCESS);
                    break;

                case ERROR:
                    this.stats = new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.ERROR);
                    break;

                default:
                    this.stats = new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.FAILED);
            }
        } else {
            this.stats = subTaskState;
        }

        this.retrievingState = doneDownload.getRetrieveState();

        //TODO: change this because it is not correct from a logical point of view !!!!
        this.processingState = !(RetrievingState.COMPLETED.equals(doneDownload.getRetrieveState())) ? ProcessingState.ERROR :
                ProcessingState.SUCCESS;
        this.log = log;
    }

    public DoneProcessing(final DoneDownload doneDownload) {
        this(doneDownload, null, null, null, null, null);
    }


    public DoneProcessing(final DoneDownload doneDownload, final ProcessingJobSubTaskStats stats,
                          final ImageMetaInfo imageMetaInfo,
                          final AudioMetaInfo audioMetaInfo, final VideoMetaInfo videoMetaInfo,
                          final TextMetaInfo textMetaInfo) {
        this(doneDownload,
                stats,
                imageMetaInfo,
                audioMetaInfo,
                videoMetaInfo,
                textMetaInfo,
                doneDownload.getHttpRetrieveResponse().getLog()
        );
    }

    public DoneProcessing(final String taskID, final String url, String referenceId, final String jobId,
                          final DocumentReferenceTaskType taskType, final Integer httpResponseCode,
                          final String httpResponseContentType, final Long httpResponseContentSizeInBytes,
                          final Long socketConnectToDownloadStartDurationInMilliSecs,
                          final Long retrievalDurationInMilliSecs, final Long checkingDurationInMilliSecs,
                          final String sourceIp, final Map<String, String> httpResponseHeaders,
                          final List<String> redirectionPath, final RetrievingState retrievingState,
                          final ProcessingState processingState, final ProcessingJobSubTaskStats stats, final String log,
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
        this.retrievingState = retrievingState;
        this.processingState = processingState;
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

    public ProcessingState getProcessingState() {
        switch (processingState) {
            case READY: /* fall through */
            case PAUSED: /* fall through */
            case DOWNLOADING: /* fall through */
            case ERROR:
                return processingState;
            case SUCCESS:
                return getSubTaskAwareProcessingState();
            default:
                /* This should never happen */
                return ProcessingState.ERROR;
        }
    }

    private ProcessingState getSubTaskAwareProcessingState() {
        switch (stats.getOverallState()) {
            case ERROR:
                return ProcessingState.ERROR;
            case FAILED:
                return ProcessingState.ERROR;
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

    public String getLog() {
        return log;
    }

    public String getTaskID() {
        return taskID;
    }

    public RetrievingState getRetrievingState() {
        return retrievingState;
    }

    public ProcessingJobSubTaskStats getProcessingStats() {
        return stats;
    }

    public DoneProcessing withNewState(ProcessingState newState, String log) {
        return new DoneProcessing(taskID, url, referenceId, jobId, taskType, httpResponseCode, httpResponseContentType,
                httpResponseContentSizeInBytes, socketConnectToDownloadStartDurationInMilliSecs,
                retrievalDurationInMilliSecs, checkingDurationInMilliSecs, sourceIp, httpResponseHeaders,
                redirectionPath, retrievingState, newState, stats, log, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);
    }

    public DoneProcessing withColorPalette(ImageMetaInfo imageMetaInfo) {
        return new DoneProcessing(taskID, url, referenceId, jobId, taskType, httpResponseCode, httpResponseContentType,
                httpResponseContentSizeInBytes, socketConnectToDownloadStartDurationInMilliSecs,
                retrievalDurationInMilliSecs, checkingDurationInMilliSecs, sourceIp, httpResponseHeaders,
                redirectionPath, retrievingState, processingState, stats, log, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);
    }
}
