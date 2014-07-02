package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.ResponseHeader;
import eu.europeana.harvester.httpclient.utils.SecureChatSslContextFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;

import javax.net.ssl.SSLEngine;
import java.util.List;

/**
 * A custom netty pipeline factory that creates constrained pipelines.
 */
class ConstrainedPipelineFactory implements ChannelPipelineFactory {

    /**
     * The interval at which the "time wheel" checks whether the limits have been reached. Must be > 0.
     */
    private final Duration limitsCheckInterval;

    /**
     * The bandwidth limit usage for write (ie. sending). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitWriteInBytesPerSec;

    /**
     * The bandwidth limit usage for read (ie. receiving). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitReadInBytesPerSec;

    /**
     * The time threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Duration terminationThresholdTimeLimit;

    /**
     * The content size threshold after which the retrieval is terminated. 0 means no limit.
     */
    private final Long terminationThresholdSizeLimitInBytes;

    /**
     * Whether to handle chunks or not. Always true.
     */
    private final Boolean handleChunks;

    /**
     * The object that stores the HTTP response.
     */
    private final HttpRetrieveResponse httpRetriveResponse;

    /**
     * The netty wheel timer shared by all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    /**
     * The task type: link check, conditional or unconditional download
     */
    private final DocumentReferenceTaskType documentReferenceTaskType;

    /**
     * Whether the pipeline supports SSL.
     */
    private final boolean supportsSSL;

    /**
     * List of headers from the last download, it's needed only if the task type is conditional download
     */
    private final List<ResponseHeader> headers;

    public ConstrainedPipelineFactory(Long bandwidthLimitReadInBytesPerSec, Long bandwidthLimitWriteInBytesPerSec,
                                      Duration limitsCheckInterval, boolean supportsSSL,
                                      Long terminationThresholdSizeLimitInBytes,
                                      Duration terminationThresholdTimeLimit, boolean handleChunks,
                                      DocumentReferenceTaskType documentReferenceTaskType,
                                      List<ResponseHeader> headers, HttpRetrieveResponse httpRetriveResponse,
                                      HashedWheelTimer hashedWheelTimer) {
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.bandwidthLimitWriteInBytesPerSec = bandwidthLimitWriteInBytesPerSec;
        this.limitsCheckInterval = limitsCheckInterval;
        this.supportsSSL = supportsSSL;
        this.terminationThresholdSizeLimitInBytes = terminationThresholdSizeLimitInBytes;
        this.terminationThresholdTimeLimit = terminationThresholdTimeLimit;
        this.handleChunks = handleChunks;
        this.documentReferenceTaskType = documentReferenceTaskType;
        this.headers = headers;
        this.httpRetriveResponse = httpRetriveResponse;
        this.hashedWheelTimer = hashedWheelTimer;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        final ChannelPipeline channelPipeline = Channels.pipeline();

        final ReadTimeoutHandler readTimeoutHandler = new ReadTimeoutHandler(hashedWheelTimer, 10);
        channelPipeline.addLast("read timeout", readTimeoutHandler);

        final ChannelTrafficShapingHandler channelTrafficShapingHandler =
                new ChannelTrafficShapingHandler(hashedWheelTimer, bandwidthLimitWriteInBytesPerSec,
                        bandwidthLimitReadInBytesPerSec, limitsCheckInterval.getMillis());
        channelPipeline.addLast("CHANNEL_TRAFFIC_SHAPING", channelTrafficShapingHandler);

        // Enable HTTPS if necessary.
        if (supportsSSL) {
            final SSLEngine engine =
                    SecureChatSslContextFactory.getClientContext().createSSLEngine();
            engine.setUseClientMode(true);

            channelPipeline.addLast("supportsSSL", new SslHandler(engine));
        }

        channelPipeline.addLast("codec", new HttpClientCodec());

        // Remove the following line if you don't want automatic content decompression.
        channelPipeline.addLast("inflater", new HttpContentDecompressor());

        // Uncomment the following line if you don't want to handle HttpChunks.
        if(!handleChunks)
            channelPipeline.addLast("aggregator", new HttpChunkAggregator(1048576));

        channelPipeline.addLast("handler",
                new HttpClientHandler(terminationThresholdSizeLimitInBytes, terminationThresholdTimeLimit,
                        hashedWheelTimer, httpRetriveResponse, documentReferenceTaskType, headers));

        return channelPipeline;
    }
}
