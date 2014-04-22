package eu.europeana.harvester.httpclient.response;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * The HTTP request response.
 */
public interface HttpRetriveResponse {

    public URL getUrl();

    public void setUrl(URL url);

    public Map<String, String> getResponseHeaders();

    public void addHeader(String type, String value);

    public ResponseState getState();

    public void setState(ResponseState responseState);

    public byte[] getContent();

    public void addContent(byte[] content) throws Exception;

    public Long getContentSizeInBytes();

    public Throwable getException();

    public void setException(Throwable exception);

    public void close() throws IOException;
}
