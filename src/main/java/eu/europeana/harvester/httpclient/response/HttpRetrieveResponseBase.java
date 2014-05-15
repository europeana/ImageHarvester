package eu.europeana.harvester.httpclient.response;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

abstract class HttpRetrieveResponseBase implements HttpRetrieveResponse {

    /**
     * The exception that caused the {@enum ResponseState.ERROR} state.
     */
    private Throwable exception;

    /**
     * The current state of the response.
     */
    private ResponseState state = ResponseState.PREPARING;

    /**
     * The url from where the data was retrieved.
     */
    private URL url;

    /**
     * The response headers.
     */
    private Map<String, String> responseHeaders = new HashMap<String, String>();

    /**
     * The size of the content in bytes. Should always be used instead of getting the content size as, depending
     * on the storage strategy, loading the content might be very expensive.
     */
    protected Long contentSizeInBytes = 0l;

    /**
     * The HTTP response code.
     */
    private Integer httpResponseCode;

    /**
     * The HTTP response content type.
     */
    private String httpResponseContentType;

    /**
     * The retrieval duration in seconds. Zero if the source is not retrieved.
     */
    private Long retrievalDurationInSecs;

    /**
     * The checking duration in seconds. The same as the retrieval if the source is retrieved.
     */
    private Long checkingDurationInSecs;

    /**
     * The IP of the source. Useful for debugging when working with DNS load balanced sources that have a pool of real
     * IP's for the same domain name.
     */
    private String sourceIp;

    /**
     * The HTTP response headers.
     */
    private ArrayList<Byte> httpResponseHeaders;

    @Override
    synchronized public ResponseState getState() {
        return state;
    }

    @Override
    synchronized public void setState(ResponseState state) {
        this.state = state;
    }

    @Override
    synchronized public URL getUrl() {
        return url;
    }

    @Override
    synchronized public void setUrl(URL url) {
        this.url = url;
    }

    @Override
    synchronized public Map<String, String> getResponseHeaders() {
        return responseHeaders;
    }

    @Override
    synchronized public void addHeader(String type, String value) {
        responseHeaders.put(type, value);
    }

    @Override
    synchronized public Long getContentSizeInBytes() {
        return contentSizeInBytes;
    }

    @Override
    synchronized public Throwable getException() {
        return exception;
    }

    @Override
    synchronized public void setException(Throwable exception) {
        this.exception = exception;
    }

    @Override
    synchronized public void close() throws IOException {
        // NO IMPLEMENTATION as by default most responses don't need to be closed.
    }

    @Override
    public Integer getHttpResponseCode() {
        return httpResponseCode;
    }

    @Override
    public void setHttpResponseCode(Integer httpResponseCode) {
        this.httpResponseCode = httpResponseCode;
    }

    @Override
    public String getHttpResponseContentType() {
        return httpResponseContentType;
    }

    @Override
    public void setHttpResponseContentType(String httpResponseContentType) {
        this.httpResponseContentType = httpResponseContentType;
    }

    @Override
    public Long getRetrievalDurationInSecs() {
        return retrievalDurationInSecs;
    }

    @Override
    public void setRetrievalDurationInSecs(Long retrievalDurationInSecs) {
        this.retrievalDurationInSecs = retrievalDurationInSecs;
    }

    @Override
    public Long getCheckingDurationInSecs() {
        return checkingDurationInSecs;
    }

    @Override
    public void setCheckingDurationInSecs(Long checkingDurationInSecs) {
        this.checkingDurationInSecs = checkingDurationInSecs;
    }

    @Override
    public String getSourceIp() {
        return sourceIp;
    }

    @Override
    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    @Override
    public ArrayList<Byte> getHttpResponseHeaders() {
        return httpResponseHeaders;
    }

    @Override
    public void addHttpResponseHeaders(String name, String value) {
        if(httpResponseHeaders == null) {
            httpResponseHeaders = new ArrayList<Byte>();
        }

        String header = name + "=" + value + "\n";

        byte[] bHeader = header.getBytes();

        for(Byte b : bHeader) {
            httpResponseHeaders.add(b);
        }
    }
}
