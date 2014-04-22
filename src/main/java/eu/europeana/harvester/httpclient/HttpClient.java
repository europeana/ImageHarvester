package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.httpclient.response.HttpRetriveResponse;
import eu.europeana.harvester.httpclient.response.ResponseState;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.util.HashedWheelTimer;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;

public class HttpClient implements Callable<HttpRetriveResponse> {

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
    private final HttpRetriveResponse httpRetriveResponse;

    /**
     * The url from where data is retrieved.
     */
    private final URL url;

    /**
     * The HTTP request.
     */
    private final HttpRequest httpRequest;

    public HttpClient(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer, HttpRetrieveConfig httpRetrieveConfig, HttpRetriveResponse httpRetriveResponse, HttpRequest httpRequest) throws MalformedURLException {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveConfig = httpRetrieveConfig;
        this.httpRetriveResponse = httpRetriveResponse;
        this.httpRequest = httpRequest;
        this.url = new URL(httpRequest.getUri());
    }

    @Override
    public HttpRetriveResponse call() throws Exception {
        httpRetriveResponse.setUrl(url);
        final ChannelPipelineFactory pipelineFactory =
                new ConstrainedPipelineFactory(httpRetrieveConfig.getBandwidthLimitReadInBytesPerSec(),
                        httpRetrieveConfig.getBandwidthLimitWriteInBytesPerSec(),
                        httpRetrieveConfig.getLimitsCheckInterval(), url.getProtocol().startsWith("https"),
                        httpRetrieveConfig.getTerminationThresholdSizeLimitInBytes(),
                        httpRetrieveConfig.getTerminationThresholdTimeLimit(), httpRetrieveConfig.getHandleChunks(), httpRetriveResponse, hashedWheelTimer);

        final ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(pipelineFactory);

        final Integer port = (url.getPort() == -1) ? url.getDefaultPort() : url.getPort();

        final ChannelFuture channelFuture = bootstrap.connect(new InetSocketAddress(url.getHost(), port));

        channelFuture.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    channel = future.getChannel();
                    channel.write(httpRequest);

                    channel.getCloseFuture().addListener(new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture channelFuture) throws Exception {
                            httpRetriveResponse.setState(ResponseState.COMPLETED);
                            try {
//                                bootstrap.releaseExternalResources();
//                                bootstrap.shutdown();
                            } catch (Exception e) {
                                httpRetriveResponse.setState(ResponseState.ERROR);
                                httpRetriveResponse.setException(e);
                                throw e;
                            }

                        }
                    });
                }
            }
        });

        return httpRetriveResponse;
    }
}


