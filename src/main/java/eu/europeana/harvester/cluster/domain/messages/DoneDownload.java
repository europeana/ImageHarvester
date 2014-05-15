package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;

public class DoneDownload implements Serializable {

    private final URL url;

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

    public DoneDownload(URL url, String jobId, Integer httpResponseCode, String httpResponseContentType,
                        Long httpResponseContentSizeInBytes, Long retrievalDurationInSecs, Long checkingDurationInSecs,
                        String sourceIp, ArrayList<Byte> httpResponseHeaders) {
        this.url = url;
        this.jobId = jobId;
        this.httpResponseCode = httpResponseCode;
        this.httpResponseContentType = httpResponseContentType;
        this.httpResponseContentSizeInBytes = httpResponseContentSizeInBytes;
        this.retrievalDurationInSecs = retrievalDurationInSecs;
        this.checkingDurationInSecs = checkingDurationInSecs;
        this.sourceIp = sourceIp;
        this.httpResponseHeaders = httpResponseHeaders;
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
}
