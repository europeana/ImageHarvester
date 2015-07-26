package eu.europeana.harvester.cluster.slave.downloading;

import com.ning.http.client.*;
import eu.europeana.harvester.cluster.Slave;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SlaveDownloader {

    private Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    public HttpRetrieveResponse downloadAndStoreInHttpRetrieveResponse(final HttpRetrieveResponse httpRetrieveResponse, final RetrieveUrl task) {

        if ((task.getDocumentReferenceTask().getTaskType() != DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD) &&
                (task.getDocumentReferenceTask().getTaskType() != DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD)) {
            throw new IllegalArgumentException("The downloader can handle only condition & unconditional downloads. Cannot handle " + task.getDocumentReferenceTask().getTaskType());
        }

        if (task.getUrl() == null || "".equalsIgnoreCase(task.getUrl())) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                    "Retrieval url failed because the downloader cannot retrieve null or 'empty string' url.");
            httpRetrieveResponse.setState(RetrievingState.ERROR);
            httpRetrieveResponse.setLog("Retrieval url failed because the downloader cannot retrieve null or 'empty string' url.");
            return httpRetrieveResponse;
        }

        final AsyncHttpClient asyncHttpClient = new AsyncHttpClient(new AsyncHttpClientConfig.Builder()
                .setMaxRedirects(task.getLimits().getRetrievalMaxNrOfRedirects())
                .setFollowRedirect(true)
                .setConnectTimeout(100000)
                .setAcceptAnyCertificate(true)
                .setMaxRequestRetry(3)
                .build());
        httpRetrieveResponse.setState(RetrievingState.PROCESSING);

        final long connectionSetupStartTimestamp = System.currentTimeMillis();

        final ListenableFuture<Integer> downloadListener = asyncHttpClient.prepareGet(task.getUrl()).execute(new AsyncHandler<Integer>() {
            final TimeWindowCounter timeWindowCounter = new TimeWindowCounter();

            @Override
            public STATE onStatusReceived(HttpResponseStatus status) throws Exception {

                final long connectionSetupDurationInMillis = System.currentTimeMillis() - connectionSetupStartTimestamp;
                httpRetrieveResponse.setSocketConnectToDownloadStartDurationInMilliSecs(connectionSetupDurationInMillis);
                httpRetrieveResponse.setCheckingDurationInMilliSecs(connectionSetupDurationInMillis);

                httpRetrieveResponse.setUrl(new URL(task.getUrl()));
                httpRetrieveResponse.setSourceIp(Slave.URL_RESOLVER.resolveIpOfUrl(task.getUrl()));

                if (connectionSetupDurationInMillis > task.getLimits().getRetrievalConnectionTimeoutInMillis()) {
                    /* Initial connection setup time longer than threshold. */
                    httpRetrieveResponse.setState(RetrievingState.FINISHED_TIME_LIMIT);
                    httpRetrieveResponse.setLog("Download aborted as connection setup duration " + connectionSetupDurationInMillis + " ms was greater than maximum configured  " + task.getLimits().getRetrievalConnectionTimeoutInMillis() + " ms");
                    return STATE.ABORT;
                }


                    /* We don't care what kind of status code it has at this moment as we will decide what to
                     * do on it only after the response headers have been received.
                     */
                httpRetrieveResponse.setHttpResponseCode(status.getStatusCode());

                return STATE.CONTINUE;
            }

            @Override
            public STATE onHeadersReceived(HttpResponseHeaders downloadResponseHeaders) throws Exception {

                final long downloadDurationInMillis = System.currentTimeMillis() - connectionSetupStartTimestamp;
                httpRetrieveResponse.setRetrievalDurationInMilliSecs(downloadDurationInMillis);

                    /* Collect the response headers */
                for (final Map.Entry<String, List<String>> entry : downloadResponseHeaders.getHeaders()) {
                    for (final String header : entry.getValue()) {
                        httpRetrieveResponse.addHttpResponseHeaders(entry.getKey(), header);
                    }
                }

                /** We terminate the connection in case of HTTP error only after we collect the response headers */
                if (httpRetrieveResponse.getHttpResponseCode() >= 400) {
                    httpRetrieveResponse.setState(RetrievingState.ERROR);
                    httpRetrieveResponse.setLog("HTTP >=400 code received. Connection aborted after response headers collected.");
                    return STATE.ABORT;
                }

                /** Abort when conditional download and headers match */
                if (task.getDocumentReferenceTask().getTaskType() == DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD) {
                    final String existingContentLength = fetchContentLengthHeader(task.getHeaders());
                    final String downloadContentLength = downloadResponseHeaders.getHeaders().getFirstValue("Content-Length"); //case insensitive map

                    if (existingContentLength != null && downloadContentLength != null &&
                        existingContentLength.trim().equalsIgnoreCase(downloadContentLength.trim())) {
                        // Same content length response headers => abort
                        httpRetrieveResponse.setState(RetrievingState.COMPLETED);
                        httpRetrieveResponse.setLog("Conditional download aborted as existing Content-Length == " + existingContentLength + " and download Content-Length == " + downloadContentLength);
                        return STATE.ABORT;
                    }
                }

                timeWindowCounter.start();

                return STATE.CONTINUE;
            }

            @Override
            public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {

                final long downloadDurationInMillis = System.currentTimeMillis() - connectionSetupStartTimestamp;
                httpRetrieveResponse.setRetrievalDurationInMilliSecs(downloadDurationInMillis);

                if (downloadDurationInMillis > task.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis()) {
                    /* Download duration longer than threshold. */
                    httpRetrieveResponse.setState(RetrievingState.FINISHED_TIME_LIMIT);
                    httpRetrieveResponse.setLog("Download aborted as it's duration " + downloadDurationInMillis + " ms was greater than maximum configured  " + task.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis() + " ms");
                    return STATE.ABORT;
                }

                if ((timeWindowCounter.previousTimeWindowRate() != -1) && (timeWindowCounter.previousTimeWindowRate() < task.getLimits().getRetrievalTerminationThresholdReadPerSecondInBytes())) {
                        /* Abort early if download is throttled by the sender. */
                    httpRetrieveResponse.setState(RetrievingState.FINISHED_RATE_LIMIT);
                    httpRetrieveResponse.setLog("Download aborted as it was throttled by the sender to " + timeWindowCounter.currentTimeWindowRate() + " bytes during the last " + timeWindowCounter.getTimeWindowSizeInSeconds() + ". This was greater than the minimum configured  " + task.getLimits().getRetrievalTerminationThresholdReadPerSecondInBytes() + "  bytes / sec");
                    return STATE.ABORT;
                }

                timeWindowCounter.incrementCount(bodyPart.length());
                httpRetrieveResponse.addContent(bodyPart.getBodyPartBytes());
                return STATE.CONTINUE;

            }

            @Override
            public Integer onCompleted() throws Exception {

                // Mark it as completed only when the previous state was processing. Otherwise it finished with a non-error state that must be kept.
                if (httpRetrieveResponse.getState() == RetrievingState.PROCESSING)
                    httpRetrieveResponse.setState(RetrievingState.COMPLETED);
                httpRetrieveResponse.setRetrievalDurationInMilliSecs(System.currentTimeMillis() - connectionSetupStartTimestamp);
                try {
                    httpRetrieveResponse.close();
                } catch (IOException e1) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                            "Failed to close the download response. This might be a bug in harvester slave code.", e1);
                }
                return 0;
            }

            @Override
            public void onThrowable(Throwable e) {

                // Remove the possibly incomplete file stored on disk
                try {
                    final File retrievalFileStorage = Paths.get(httpRetrieveResponse.getAbsolutePath()).toFile();
                    if (retrievalFileStorage.exists()) {
                        retrievalFileStorage.delete();
                    }
                } catch (Exception e1) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                            "Cleaning of the incomplete download result file failed in an unexpected way. This might be a bug in harvester slave code.", e1);
                }

                httpRetrieveResponse.setState(RetrievingState.ERROR);

                // Check if the tim threshold limit was exceeded & save that information.
                final long downloadDurationInMillis = System.currentTimeMillis() - connectionSetupStartTimestamp;
                if (downloadDurationInMillis > task.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis()) {
                    /* Download duration longer than threshold. */
                    httpRetrieveResponse.setState(RetrievingState.FINISHED_TIME_LIMIT);
                    httpRetrieveResponse.setLog("Download aborted as it's duration " + downloadDurationInMillis + " ms was greater than maximum configured  " + task.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis() + " ms");
                }

                // Check if it was aborted because of conditional download with with same headers.
                if (httpRetrieveResponse.getState() == RetrievingState.COMPLETED && task.getDocumentReferenceTask().getTaskType() == DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD) {
                    // We don't set any exception as the download was aborted for a legitimate reason.
                    cleanup(httpRetrieveResponse,task, asyncHttpClient, httpRetrieveResponse.getException());
                } else {
                    // We set the exception as the download was aborted because of a problem.
                    cleanup(httpRetrieveResponse,task, asyncHttpClient, e);
                }
            }
        });

        try {
            Integer r = downloadListener.get(1, TimeUnit.DAYS /* This timeout should never be reached. There are other timeouts used internally that will expire much quicker. */);
            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                    "Download finished with status {}", r);

        } catch (Exception e) {
            cleanup(httpRetrieveResponse,task, asyncHttpClient, e);

        } finally {
            cleanup(httpRetrieveResponse,task, asyncHttpClient, httpRetrieveResponse.getException());
            asyncHttpClient.close();
            return httpRetrieveResponse;
        }
    }

    private String fetchContentLengthHeader (Map<String, String> headers) {
        for (final Map.Entry<String, String> entry: headers.entrySet()) {
            if ("Content-Length".equalsIgnoreCase(entry.getKey())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private void cleanup(final HttpRetrieveResponse httpRetrieveResponse,final RetrieveUrl task, final AsyncHttpClient asyncHttpClient, final Throwable e) {
        if (httpRetrieveResponse != null) httpRetrieveResponse.setException(e);
        try {
            if (httpRetrieveResponse != null) httpRetrieveResponse.close();
        } catch (IOException e1) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                    "Failed to execute the final cleanup of the async downloader ", e1);
        }
    }

}
