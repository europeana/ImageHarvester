package eu.europeana.harvester.httpclient.response;

import eu.europeana.harvester.domain.ResponseHeader;
import net.logstash.logback.marker.MapEntriesAppendingMarker;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * The HTTP request response.
 */
 public interface HttpRetrieveResponse extends Serializable {

     URL getUrl();

     void setUrl(URL url);

     void init() throws IOException;

     String getAbsolutePath();

     Map<String, String> getResponseHeaders();

     void addHeader(String type, String value);

     RetrievingState getState();

     void setState(RetrievingState retrievingState);

     byte[] getContent() throws IOException;

     void addContent(byte[] content) throws Exception;

     Long getContentSizeInBytes();

     Throwable getException();

     void setException(Throwable exception);

     void close() throws IOException;

     Integer getHttpResponseCode();

     void setHttpResponseCode(Integer httpResponseCode);

     String getHttpResponseContentType();

     void setHttpResponseContentType(String httpResponseContentType);

     Long getSocketConnectToDownloadStartDurationInMilliSecs();

     void setSocketConnectToDownloadStartDurationInMilliSecs(Long socketConnectToDownloadStartDurationInSecs);

     Long getRetrievalDurationInMilliSecs();

     void setRetrievalDurationInMilliSecs(Long retrievalDurationInSecs);

     Long getCheckingDurationInMilliSecs();

     void setCheckingDurationInMilliSecs(Long checkingDurationInSecs);

     String getSourceIp();

     void setSourceIp(String sourceIp);

     List<ResponseHeader> getHttpResponseHeaders();

     void addHttpResponseHeaders(String name, String value);

     List<String> getRedirectionPath();

     void setRedirectionPath(List<String> redirectionPath);

     void addRedirectionPath(String redirectionPath);

     String getLog();

     void setLog(String log);

    void setLoggingAppFields (MapEntriesAppendingMarker mapEntriesAppendingMarker);
}
