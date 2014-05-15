package eu.europeana.harvester.httpclient.response;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;

/**
 * The HTTP request response.
 */
public interface HttpRetrieveResponse extends Serializable {

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

    public Integer getHttpResponseCode();

    public void setHttpResponseCode(Integer httpResponseCode);

    public String getHttpResponseContentType();

    public void setHttpResponseContentType(String httpResponseContentType);

    public Long getRetrievalDurationInSecs();

    public void setRetrievalDurationInSecs(Long retrievalDurationInSecs);

    public Long getCheckingDurationInSecs();

    public void setCheckingDurationInSecs(Long checkingDurationInSecs);

    public String getSourceIp();

    public void setSourceIp(String sourceIp);

    public ArrayList<Byte> getHttpResponseHeaders();

    public void addHttpResponseHeaders(String name, String value);

}
