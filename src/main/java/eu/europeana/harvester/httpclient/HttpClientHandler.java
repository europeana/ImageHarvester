package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.httpclient.response.HttpRetriveResponse;
import eu.europeana.harvester.httpclient.response.ResponseState;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.joda.time.Duration;

import java.util.concurrent.TimeUnit;

/**
 * Manages the HTTP client request process enforcing the limits set on download amount and time.
 */
public class HttpClientHandler extends SimpleChannelHandler {

    /**
     * Holds the total count of bytes downloaded in the current request. Used to enforce premature termination of
     * the request if the content size limits have been exceeded.
     */
    private long totalContentBytesRead = 0;

    /**
     * Whether the request can handle chunked transfers.
     */
    private boolean readingChunks;

    /**
     * Whether the request should stop after it has retrieved a specific number of bytes.
     */
    private final boolean hasSizeLimitsForContent;

    /**
     * Whether the request should stop after it retrieved content for a specific duration.
     */
    private final boolean hasTimeLimitsForContent;

    /**
     * The size limits in bytes for content. ZERO if no limit.
     */
    private final long sizeLimitsInBytesForContent;

    /**
     * The duration limits for content retrieval. Duration.ZERO if no limit.
     */
    private final Duration timeLimitForContentRetrieval;

    /**
     * Holds the http response where all the retrieved content ends up. Also usefull for progress inspection during
     * the process.
     */
    private final HttpRetriveResponse httpRetriveResponse;

    public HttpClientHandler(final Long sizeLimitsInBytesForContent, final Duration timeLimitForContentRetrieval,
                             final HashedWheelTimer hashedWheelTimer, final HttpRetriveResponse httpRetriveResponse) {
        this.httpRetriveResponse = httpRetriveResponse;

        this.hasSizeLimitsForContent = (sizeLimitsInBytesForContent != 0);
        this.hasTimeLimitsForContent = (timeLimitForContentRetrieval != Duration.ZERO);
        this.sizeLimitsInBytesForContent = sizeLimitsInBytesForContent;
        this.timeLimitForContentRetrieval = timeLimitForContentRetrieval;

        if (hasTimeLimitsForContent) {
        /* Limit the time of download. */
            final TimerTask timerTask = new TimerTask() {
                public void run(final Timeout timeout) throws Exception {
                    if (totalContentBytesRead != 0) {
                        httpRetriveResponse.setState(ResponseState.FINISHED_TIME_LIMIT);
                    }
                }
            };
            hashedWheelTimer.newTimeout(timerTask, timeLimitForContentRetrieval.getStandardSeconds(), TimeUnit.SECONDS);
        }
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
        httpRetriveResponse.setState(ResponseState.PROCESSING);
        if (!readingChunks) {
            final HttpResponse response = (HttpResponse) e.getMessage();

            totalContentBytesRead += response.getStatus().toString().length() + response.getProtocolVersion().toString().length();

            if (!response.headers().names().isEmpty()) {
                for (final String name : response.headers().names()) {
                    for (final String value : response.headers().getAll(name)) {
                        httpRetriveResponse.addHeader(name, value);
                        totalContentBytesRead += name.length() + value.length();
                    }
                }
            }

            if (response.isChunked()) {
                readingChunks = true;
            } else {
                final ChannelBuffer content = response.getContent();
                if (content.readable()) {
                    httpRetriveResponse.addContent(response.getContent().array());
                }
            }
        } else {
            final HttpChunk chunk = (HttpChunk) e.getMessage();
            httpRetriveResponse.addContent(chunk.getContent().array());
            totalContentBytesRead += chunk.getContent().toString(CharsetUtil.UTF_8).length();

            if (chunk.isLast()) {
                readingChunks = false;
            }
        }

        if (hasSizeLimitsForContent && totalContentBytesRead > sizeLimitsInBytesForContent) {
            httpRetriveResponse.setState(ResponseState.FINISHED_SIZE_LIMIT);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        httpRetriveResponse.setState(ResponseState.ERROR);
        httpRetriveResponse.setException(e.getCause());
        ctx.sendUpstream(e);
    }
}