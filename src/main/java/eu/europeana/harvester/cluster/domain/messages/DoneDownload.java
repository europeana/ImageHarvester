package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseHeader;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Message sent by slave actor and node master actor after the download is done.
 */
public class DoneDownload implements Serializable {

    /**
     * The url.
     */
    private final URL url;

    /**
     * The caller jobs id.
     */
    private final String jobId;

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
     * List of urls until we reach the final url.
     */
    private final List<String> redirectionPath;

    /**
     * Stores meta information about an image type document (width, height, mimetype, fileformat, colorspace).
     */
    private final ImageMetaInfo imageMetaInfo;

    /**
     * Stores meta information about an audio type document (samplerate, bitrate, duration, fileformat, mimetype).
     */
    private final AudioMetaInfo audioMetaInfo;

    /**
     * Stores meta information about a video type document (width, height, duration, fileformat, framerate, mimetype).
     */
    private final VideoMetaInfo videoMetaInfo;

    private final ProcessingState processingState;

    public DoneDownload(URL url, String jobId, Integer httpResponseCode, String httpResponseContentType,
                        Long httpResponseContentSizeInBytes, Long socketConnectToDownloadStartDurationInMilliSecs,
                        Long retrievalDurationInMilliSecs, Long checkingDurationInMilliSecs, String sourceIp,
                        List<ResponseHeader> httpResponseHeaders, List<String> redirectionPath,
                        ImageMetaInfo imageMetaInfo, AudioMetaInfo audioMetaInfo, VideoMetaInfo videoMetaInfo, ProcessingState processingState) {
        this.url = url;
        this.jobId = jobId;
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
        this.processingState = processingState;
    }

    public URL getUrl() {
        return url;
    }

    public String getJobId() {
        return jobId;
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

    public ProcessingState getProcessingState() {
        return processingState;
    }
}
