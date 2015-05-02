package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.request.HttpHEAD;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.utils.CallbackInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.*;
import java.util.Map;
import java.util.concurrent.Callable;

public class HttpClient implements Callable<HttpRetrieveResponse> {

    private static final Logger LOG = LogManager.getLogger(HttpClient.class.getName());

    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    /**
     * The Channel used in this client instance.
     */
    private Channel channel;

    /**
     * The config used to constrain the request.
     */
    private final HttpRetrieveConfig httpRetrieveConfig;

    /**
     * The request response.
     */
    private final HttpRetrieveResponse httpRetrieveResponse;

    /**
     * The url from where data is retrieved.
     */
    private final URL url;

    private ChannelFuture closeFuture;

    /**
     * List of headers from the last download, it's needed only if the task type is conditional download
     */
    private final Map<String, String> headers;

    /**
     * The maximum number of allowed levels of redirects.
     */
    private final Integer maxRedirects;

    public HttpClient(final ChannelFactory channelFactory, final HashedWheelTimer hashedWheelTimer,
                      final HttpRetrieveConfig httpRetrieveConfig, final Map<String, String> headers,
                      final HttpRetrieveResponse httpRetrieveResponse, final String link) throws MalformedURLException {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.headers = headers;
        this.httpRetrieveResponse = httpRetrieveResponse;
        this.maxRedirects = httpRetrieveConfig.getMaxNrOfRedirects();
        this.url = new URL(link);

        startDownload();
    }

    /**
     * Collects some information about the target url and then  starts the download.
     */
    private void startDownload() {
        try {
            handleRedirects();
        } catch (Exception e) {
            LOG.error("In HttpClient: exception in handleRedirects");
            try {
                httpRetrieveResponse.close();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
            return;
        }
        if(httpRetrieveResponse.getHttpResponseCode() == -1) {
            try {
                httpRetrieveResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return;
        }

        final URL newURL = httpRetrieveResponse.getUrl();

        LOG.info("Time termination limit = {}", httpRetrieveConfig.getTerminationThresholdTimeLimit());

        final ChannelPipelineFactory pipelineFactory =
                new ConstrainedPipelineFactory(
                        httpRetrieveConfig.getLimitsCheckInterval(),
                        httpRetrieveConfig.getConnectionTimeoutInMillis(),
                        newURL.getProtocol().startsWith("https"),
                        httpRetrieveConfig.getTerminationThresholdSizeLimitInBytes(),
                        httpRetrieveConfig.getTerminationThresholdTimeLimit(), httpRetrieveConfig.getHandleChunks(),
                        httpRetrieveConfig.getTaskType(), headers, httpRetrieveResponse, hashedWheelTimer);

        final ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(pipelineFactory);

        final Integer port = (newURL.getPort() == -1) ? newURL.getDefaultPort() : newURL.getPort();

        final ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(newURL.getHost(), port));

        try {
            channelFuture.await();
        } catch (InterruptedException e) {
            LOG.error(e.getMessage());
            channelFuture.getChannel().close();
            try {
                httpRetrieveResponse.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return;
        }
        channel = channelFuture.getChannel();

        try {
            final String address = String.valueOf(channel.getRemoteAddress()).split("/")[1].split(":")[0];
            httpRetrieveResponse.setSourceIp(address);
        } catch (Exception e) {
            httpRetrieveResponse.setLog("In HttpClient: error at getting IP address");
            httpRetrieveResponse.setHttpResponseCode(-1);
            try {
                httpRetrieveResponse.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            channel.close();
            return;
        }

        if((DocumentReferenceTaskType.CHECK_LINK).equals(httpRetrieveConfig.getTaskType())) {
            channel.write(HttpHEAD.build(newURL));
        } else {
            channel.write(HttpGET.build(newURL));
        }

        closeFuture = channel.getCloseFuture();
    }

    /**
     * Follows redirects.
     */
    private void handleRedirects() {
        URL tempUrl = url;

        int depth = 0;
        boolean redirect;
        do {
            redirect = false;
            httpRetrieveResponse.setUrl(tempUrl);
            HttpURLConnection httpURLConnection = null;
            try {
                httpURLConnection = (HttpURLConnection) tempUrl.openConnection();
                httpURLConnection.setRequestMethod("GET");
                httpURLConnection.setInstanceFollowRedirects(false);
                httpURLConnection.setConnectTimeout(httpRetrieveConfig.getConnectionTimeoutInMillis());

                try {
                    long start = System.currentTimeMillis();
                    httpURLConnection.connect();
                    httpRetrieveResponse.
                            setSocketConnectToDownloadStartDurationInMilliSecs(System.currentTimeMillis() - start);
                    int responseCode = httpURLConnection.getResponseCode();
                    httpRetrieveResponse.setHttpResponseCode(responseCode);
                    if(responseCode >= 300 && responseCode < 400) {
                        final String redirectURL = httpURLConnection.getHeaderField("Location");
                        httpRetrieveResponse.addRedirectionPath(redirectURL);
                    }

                    if(httpRetrieveResponse.getHttpResponseCode() == null) {
                        httpRetrieveResponse.setHttpResponseCode(-1);
                        httpRetrieveResponse.setLog("In HttpClient: HttpResponseCode is null");
                    }
                    if(httpRetrieveResponse.getHttpResponseCode() == -1) return;
                } catch (Exception e) {
                    httpRetrieveResponse.setHttpResponseCode(-1);
                    httpRetrieveResponse.setLog("In HttpClient: \n" + e.toString());
                    return;
                } finally {
                    httpURLConnection.disconnect();
                }

            } catch (ClassCastException e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In HttpClient: \n" + e.toString());
                return;
            } catch (UnknownHostException e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In HttpClient: \n" + e.toString());
                return;
            } catch (IOException e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In HttpClient: \n" + e.toString());
                return;
            } finally {
                if(httpURLConnection != null) {
                    httpURLConnection.disconnect();
                }
            }

            if(httpRetrieveResponse.getRedirectionPath().size() != depth) {
                redirect = true;
                try {
                    tempUrl = new URL(httpRetrieveResponse.getRedirectionPath().get(depth));
                } catch (MalformedURLException e) {
                    httpRetrieveResponse.setHttpResponseCode(-1);
                    httpRetrieveResponse.setLog("In HttpClient: \n" + e.toString());
                    return;
                }
                depth++;
            }
            if(maxRedirects <= depth) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In HttpClient: to many redirects");
                return;
            }

        } while(redirect);
    }

    @Override
    public HttpRetrieveResponse call() {
        try {
            if(closeFuture == null) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In HttpClient: closeFuture is null");
            } else {
                closeFuture.await();
                httpRetrieveResponse.setUrl(url);
            }
        } catch (InterruptedException e) {
            httpRetrieveResponse.setState(ResponseState.ERROR);
            httpRetrieveResponse.setLog("In HttpClient: \n" + e.toString());
        }

        try {
            httpRetrieveResponse.close();
        } catch (IOException e) {
            LOG.error("In HttpClient: exception while closing the fileInputStream.");
        }

        if(closeFuture != null) {
            closeFuture.cancel();
            channel.close();
        }
        if(master != null) {
            master.returnResult(httpRetrieveResponse);
        }
        return httpRetrieveResponse;
    }

    private CallbackInterface master;

    public void setMaster(CallbackInterface master) {
        this.master = master;
    }

    public CallbackInterface getMaster() {
        return master;
    }

}
