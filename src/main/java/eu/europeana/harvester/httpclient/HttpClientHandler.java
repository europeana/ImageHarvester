package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.ResponseHeader;
import eu.europeana.harvester.httpclient.response.ResponseState;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.util.CharsetUtil;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manages the HTTP client request process enforcing the limits set on download amount and time.
 */
class HttpClientHandler extends SimpleChannelHandler {

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
    private final HttpRetrieveResponse httpRetrieveResponse;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    /**
     * The task type: link check, conditional or unconditional download
     */
    private final DocumentReferenceTaskType documentReferenceTaskType;

    /**
     * A flag which shows that the arriving package is the first or not
     */
    private Boolean firstPackage;

    private final long connectionStartTime;

    private long downloadStartTime;

    /**
     * The response headers.
     */
    private final List<ResponseHeader> headers;

    private long firstPackageArriveTime;

    public HttpClientHandler(final Long sizeLimitsInBytesForContent, final Duration timeLimitForContentRetrieval,
                             final HashedWheelTimer hashedWheelTimer, final HttpRetrieveResponse httpRetrieveResponse,
                             DocumentReferenceTaskType documentReferenceTaskType, List<ResponseHeader> headers) {
        this.httpRetrieveResponse = httpRetrieveResponse;
        this.documentReferenceTaskType = documentReferenceTaskType;
        this.headers = headers;

        this.hasSizeLimitsForContent = (sizeLimitsInBytesForContent != 0);
        this.hasTimeLimitsForContent = (timeLimitForContentRetrieval.getMillis() != 0);
        this.sizeLimitsInBytesForContent = sizeLimitsInBytesForContent;
        this.timeLimitForContentRetrieval = timeLimitForContentRetrieval;

        this.hashedWheelTimer = hashedWheelTimer;

        this.firstPackage = true;

        this.connectionStartTime = System.currentTimeMillis();
    }

    /**
     * Starts a timer if we have a time limit on download.
     * @param channel in this channel come all the packages
     */
    private void startTimer(final Channel channel) {
        /* Limit the time of download. */
        final TimerTask timerTask = new TimerTask() {
            public void run(final Timeout timeout) throws Exception {
                if (totalContentBytesRead != 0) {
                    httpRetrieveResponse.setState(ResponseState.FINISHED_TIME_LIMIT);
                    httpRetrieveResponse.setCheckingDurationInMilliSecs(firstPackageArriveTime - connectionStartTime);
                    httpRetrieveResponse.setRetrievalDurationInMilliSecs(0l);

                    channel.close();
                }
            }
        };

        hashedWheelTimer.newTimeout(timerTask, timeLimitForContentRetrieval.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) throws Exception {
        firstPackageArriveTime = System.currentTimeMillis();
        httpRetrieveResponse.setState(ResponseState.PROCESSING);

        if (!readingChunks) {
            if (hasTimeLimitsForContent && firstPackage) {
                firstPackage = false;
                startTimer(ctx.getChannel());
            }

            final HttpResponse response = (HttpResponse) e.getMessage();

            if (!response.headers().names().isEmpty()) {
                handleHeaders(response, ctx);
            }

            handleDifferentTaskTypes(ctx);

            if (response.isChunked()) {
                readingChunks = true;
            } else {
                final ChannelBuffer content = response.getContent();
                if (content.readable()) {
                    httpRetrieveResponse.addContent(response.getContent().array());
                    httpRetrieveResponse.setState(ResponseState.COMPLETED);
                }
            }
        } else {
            final HttpChunk chunk = (HttpChunk) e.getMessage();
            httpRetrieveResponse.addContent(chunk.getContent().array());
            totalContentBytesRead += chunk.getContent().toString(CharsetUtil.UTF_8).length();

            if (chunk.isLast()) {
                readingChunks = false;
                httpRetrieveResponse.setState(ResponseState.COMPLETED);
                httpRetrieveResponse.setRetrievalDurationInMilliSecs(System.currentTimeMillis() - downloadStartTime);
                httpRetrieveResponse.setCheckingDurationInMilliSecs(0l);
            }
        }

        if (hasSizeLimitsForContent && totalContentBytesRead > sizeLimitsInBytesForContent) {
            httpRetrieveResponse.setState(ResponseState.FINISHED_SIZE_LIMIT);
            httpRetrieveResponse.setCheckingDurationInMilliSecs(firstPackageArriveTime - connectionStartTime);
            httpRetrieveResponse.setRetrievalDurationInMilliSecs(0l);

            ctx.getChannel().close();
        }
    }

    /**
     * This method saves the headers and checks if there is a redirect. If the url is redirected then stops the downloa.
     * @param response
     * @param ctx
     */
    private void handleHeaders(HttpResponse response, final ChannelHandlerContext ctx) {
        // Reads the headers
        for (final String name : response.headers().names()) {
            for (final String value : response.headers().getAll(name)) {
                httpRetrieveResponse.addHeader(name, value);
                httpRetrieveResponse.addHttpResponseHeaders(name, value);

                if(name.equals("Content-Type")) {
                    httpRetrieveResponse.setHttpResponseContentType(value);
                }

                // Redirect url
                if(name.equals("Location")) {
                    httpRetrieveResponse.addRedirectionPath(value);
                    System.out.println("Redirect");

                    ctx.getChannel().close();
                }
            }
        }
    }

    /**
     * There are 3 different type of download. This method treats them.
     * @param ctx
     */
    private void handleDifferentTaskTypes(final ChannelHandlerContext ctx) {
        switch(documentReferenceTaskType) {
            case CHECK_LINK:
                httpRetrieveResponse.setState(ResponseState.COMPLETED);
                httpRetrieveResponse.setRetrievalDurationInMilliSecs(0l);
                httpRetrieveResponse.setCheckingDurationInMilliSecs(firstPackageArriveTime - connectionStartTime);

                ctx.getChannel().close();
                break;
            case CONDITIONAL_DOWNLOAD:
                if(headers != null) {
                    boolean changedModifiedDate = false;
                    boolean changedContentLength = false;

                    ArrayList<Byte> lastModified = null;
                    ArrayList<Byte> contentLength = null;

                    for(ResponseHeader responseHeader : httpRetrieveResponse.getHttpResponseHeaders()) {
                        if(responseHeader.getKey().equals("Last-Modified")) {
                            lastModified = responseHeader.getValue();
                        } else
                        if(responseHeader.getKey().equals("Content-Length")) {
                            contentLength = responseHeader.getValue();
                        }
                    }

                    for(ResponseHeader responseHeader : headers) {
                        if(responseHeader.getKey().equals("Last-Modified")) {
                            changedModifiedDate = isHeaderChanged(lastModified, responseHeader.getValue());
                        } else
                        if(responseHeader.getKey().equals("Content-Length")) {
                            changedContentLength = isHeaderChanged(contentLength, responseHeader.getValue());
                        }
                    }

                    if(!changedModifiedDate && !changedContentLength) {
                        httpRetrieveResponse.setState(ResponseState.COMPLETED);
                        httpRetrieveResponse.setRetrievalDurationInMilliSecs(0l);
                        httpRetrieveResponse.setCheckingDurationInMilliSecs(0l);

                        ctx.getChannel().close();
                    }
                }
                break;
            case UNCONDITIONAL_DOWNLOAD:
                break;
        }

        downloadStartTime = System.currentTimeMillis();
    }

    /**
     * If the task type is conditional download we download the content only if there something has changed
     * @param oldHeader old header content
     * @param newHeader new header content
     * @return true if changed false otherwise
     */
    private boolean isHeaderChanged(ArrayList<Byte> oldHeader, ArrayList<Byte> newHeader) {
        if(oldHeader == null) return false;
        if(newHeader == null) return true;
        if(oldHeader.size() != newHeader.size()) return true;

        for(int i=0; i<oldHeader.size(); i++) {
            if(!oldHeader.get(i).equals(newHeader.get(i))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
        httpRetrieveResponse.setState(ResponseState.ERROR);
        httpRetrieveResponse.setException(e.getCause());

        ctx.sendUpstream(e);
    }

}