package eu.europeana.harvester.httpclient.response;

import eu.europeana.harvester.domain.ResponseHeader;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * The HTTP request response.
 */
public interface HttpRetrieveResponse extends Serializable {

    public URL getUrl();

    public void setUrl(URL url);

    public void init();

    public String getAbsolutePath();

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

    public Long getSocketConnectToDownloadStartDurationInMilliSecs();

    public void setSocketConnectToDownloadStartDurationInMilliSecs(Long socketConnectToDownloadStartDurationInSecs);

    public Long getRetrievalDurationInMilliSecs();

    public void setRetrievalDurationInMilliSecs(Long retrievalDurationInSecs);

    public Long getCheckingDurationInMilliSecs();

    public void setCheckingDurationInMilliSecs(Long checkingDurationInSecs);

    public String getSourceIp();

    public void setSourceIp(String sourceIp);

    public List<ResponseHeader> getHttpResponseHeaders();

    public void addHttpResponseHeaders(String name, String value);

    public List<String> getRedirectionPath();

    public void setRedirectionPath(List<String> redirectionPath);

    public void addRedirectionPath(String redirectionPath);

    public String getLog();

    public void setLog(String log);

}
