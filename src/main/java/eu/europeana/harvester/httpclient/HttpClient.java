package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.ResponseHeader;
import eu.europeana.harvester.httpclient.response.ResponseState;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

public class HttpClient implements Callable<HttpRetrieveResponse> {

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

    /**
     * The HTTP request.
     */
    private final HttpRequest httpRequest;

    private ChannelFuture closeFuture;

    /**
     * List of headers from the last download, it's needed only if the task type is conditional download
     */
    private final List<ResponseHeader> headers;
long time;
    public HttpClient(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer,
                      HttpRetrieveConfig httpRetrieveConfig, List<ResponseHeader> headers,
                      HttpRetrieveResponse httpRetrieveResponse,
                      HttpRequest httpRequest) throws MalformedURLException {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.headers = headers;
        this.httpRetrieveResponse = httpRetrieveResponse;
        this.httpRequest = httpRequest;
        this.url = new URL(httpRequest.getUri());

        time = System.currentTimeMillis();

        startDownload();
    }

    private void startDownload() {
        httpRetrieveResponse.setUrl(url);
        try {
            final InetAddress address = InetAddress.getByName(url.getHost());
            httpRetrieveResponse.setSourceIp(address.getHostAddress());

            final HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setInstanceFollowRedirects(false);
            httpURLConnection.setConnectTimeout(httpRetrieveConfig.getConnectionTimeoutInMillis());

            try {
                long start = System.currentTimeMillis();
                httpURLConnection.connect();
                httpRetrieveResponse.
                        setSocketConnectToDownloadStartDurationInMilliSecs(System.currentTimeMillis() - start);
            } catch (Exception e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                return;
            }
            try {
                int responseCode = httpURLConnection.getResponseCode();
                httpRetrieveResponse.setHttpResponseCode(responseCode);
            } catch (Exception e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                return;
            }
        } catch (UnknownHostException e) {
            httpRetrieveResponse.setHttpResponseCode(-1);
            e.printStackTrace();
            return;
        } catch (IOException e) {
            e.printStackTrace();
        }
        final ChannelPipelineFactory pipelineFactory =
                new ConstrainedPipelineFactory(httpRetrieveConfig.getBandwidthLimitReadInBytesPerSec(),
                        httpRetrieveConfig.getBandwidthLimitWriteInBytesPerSec(),
                        httpRetrieveConfig.getLimitsCheckInterval(), url.getProtocol().startsWith("https"),
                        httpRetrieveConfig.getTerminationThresholdSizeLimitInBytes(),
                        httpRetrieveConfig.getTerminationThresholdTimeLimit(), httpRetrieveConfig.getHandleChunks(),
                        httpRetrieveConfig.getTaskType(), headers, httpRetrieveResponse, hashedWheelTimer);

        final ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(pipelineFactory);

        final Integer port = (url.getPort() == -1) ? url.getDefaultPort() : url.getPort();

        final ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(url.getHost(), port));

        try {
            channelFuture.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        channel = channelFuture.getChannel();
        channel.write(httpRequest);
        closeFuture = channel.getCloseFuture();
    }

    @Override
    public HttpRetrieveResponse call() {
        try {
            System.out.println("Start ");
            closeFuture.await();
            System.out.println("End in " + (System.currentTimeMillis() - time) / 1000.0);
        } catch (InterruptedException e) {
            httpRetrieveResponse.setState(ResponseState.ERROR);
            httpRetrieveResponse.setException(e);
            e.printStackTrace();
        }

        closeFuture.cancel();
        channel.close();

        return httpRetrieveResponse;
    }

}
