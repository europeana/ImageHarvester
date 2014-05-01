package eu.europeana.harvester.httpclient.response;

import java.io.IOException;
import java.net.URL;
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

}
