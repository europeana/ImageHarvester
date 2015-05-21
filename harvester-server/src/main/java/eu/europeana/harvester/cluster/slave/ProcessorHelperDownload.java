package eu.europeana.harvester.cluster.slave;

import akka.event.LoggingAdapter;
import com.ning.http.client.*;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by nutz on 21.05.2015.
 */
public class ProcessorHelperDownload {


    private LoggingAdapter LOG;

    public  ProcessorHelperDownload ( LoggingAdapter LOG) {
        this.LOG = LOG;
    }

    /**
     * Starts one download
     * @param task contains all the information for this task
     * @return - finished job response with all the collected information
     */
    public HttpRetrieveResponse downloadTask(RetrieveUrl task, String pathToSave) {
        HttpRetrieveResponse httpRetrieveResponse = null;
        final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
        final String path = pathToSave + "/" + task.getReferenceId();


        try {


            if((DocumentReferenceTaskType.CHECK_LINK).equals(task.getHttpRetrieveConfig().getTaskType())) {
                httpRetrieveResponse = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE, path);
            } else {
                httpRetrieveResponse = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, path);
            }

            final String url = task.getUrl();


            try {
                httpRetrieveResponse.setUrl(new URL(url));
            } catch (MalformedURLException e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In DownloaderSlaveActor: \n" + e.toString());
                return httpRetrieveResponse;
            }

            download(httpRetrieveResponse, task, LOG);

            return httpRetrieveResponse;

            //httpRetrieveResponse = httpClient.call();
        } catch (Exception e) {
            LOG.error(e.getMessage());
            try {
                if(httpRetrieveResponse != null) {
                    httpRetrieveResponse.close();
                }
            } catch (IOException e1) {
                LOG.error("In DownloaderSlaveActor: error at closing httpRetrieveResponse fileOutputStream.");
                e1.printStackTrace();
            }
            if(httpRetrieveResponse != null) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In DownloaderSlaveActor: \n" + e.toString());
            }
        }

        return httpRetrieveResponse;
    }


    private void download ( final HttpRetrieveResponse httpRetrieveResponse, RetrieveUrl task, final LoggingAdapter LOG ) {
        AsyncHttpClient asyncHttpClient = null;
        httpRetrieveResponse.setState(ResponseState.PROCESSING);
        final long firstPackageArriveTime = System.currentTimeMillis();



        try {

            AsyncHttpClientConfig cfg = new AsyncHttpClientConfig.Builder()//
                    .setMaxRedirects(7)//
                    .setFollowRedirect(true)//
                    .setAcceptAnyCertificate(true)//
                    .setConnectTimeout(5000)
                    .setMaxRequestRetry(3)
                    .build();

            asyncHttpClient = new AsyncHttpClient(cfg);

            ListenableFuture<Integer> f = asyncHttpClient.prepareGet(task.getUrl()).execute(new AsyncHandler<Integer>() {
                int bytesSize;
                long markTime;
                @Override
                public STATE onStatusReceived(HttpResponseStatus status) throws Exception {
                    int statusCode = status.getStatusCode();
                    httpRetrieveResponse.setHttpResponseCode(statusCode);
                    // The Status have been read
                    // If you don't want to read the headers,body or stop processing the response
                    if (statusCode >= 500) {
                        httpRetrieveResponse.setHttpResponseCode(-1);
                        httpRetrieveResponse.close();
                        return STATE.ABORT;
                    }
                    //httpRetrieveResponse.setHttpResponseCode(statusCode);
                    return STATE.CONTINUE;
                }

                @Override
                public STATE onHeadersReceived(HttpResponseHeaders h) throws Exception {
                    for (Map.Entry<String, List<String>> entry : h.getHeaders()) {
                        for ( String hder : entry.getValue()) {
                            httpRetrieveResponse.addHttpResponseHeaders(entry.getKey(), hder);
                        }
                    }

                    bytesSize=0;
                    markTime=System.currentTimeMillis();


                    return STATE.CONTINUE;
                }

                @Override
                public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                    if (System.currentTimeMillis()-markTime<5*60*1000) {
                        httpRetrieveResponse.addContent(bodyPart.getBodyPartBytes());
                        bytesSize += bodyPart.length();
                        return STATE.CONTINUE;
                    } else {
                        if (bytesSize > 2000000000) {
                            markTime = System.currentTimeMillis();
                            bytesSize = 0;
                            httpRetrieveResponse.addContent(bodyPart.getBodyPartBytes());
                            return STATE.CONTINUE;
                        } else {
                            httpRetrieveResponse.setHttpResponseCode(-1);
                            httpRetrieveResponse.close();
                            return STATE.ABORT;
                        }
                    }
                }

                @Override
                public Integer onCompleted() throws Exception {
                    // Will be invoked once the response has been fully read or a ResponseComplete exception
                    // has been thrown.
                    // NOTE: should probably use Content-Encoding from headers
                    LOG.info("Finished 1 download");
                    httpRetrieveResponse.setState(ResponseState.COMPLETED);
                    httpRetrieveResponse.setRetrievalDurationInMilliSecs(System.currentTimeMillis() - firstPackageArriveTime);
                    httpRetrieveResponse.setCheckingDurationInMilliSecs(0l);
                    try {
                        httpRetrieveResponse.close();
                    } catch ( IOException e1 ){
                        LOG.info("Failed to close the response, caused by : "+ e1.getMessage());
                    }
                    return 0;
                }

                @Override
                public void onThrowable(Throwable t) {
                    httpRetrieveResponse.setHttpResponseCode(-1);
                    httpRetrieveResponse.setLog("Exception while downloading "+t.getMessage());
                    try {
                        httpRetrieveResponse.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });

            Integer r = f.get(5, TimeUnit.MINUTES);
            asyncHttpClient.close();

            LOG.info("Download finished with status {}", r);

            //r.getResponseBodyAsStream();
        } catch ( Exception e) {
            LOG.error("Error in download: {} ",e.getMessage());
            try {
                if(httpRetrieveResponse != null) {
                    httpRetrieveResponse.close();
                }
            } catch (IOException e1) {
                LOG.error("In DownloaderSlaveActor: error at closing httpRetrieveResponse fileOutputStream.");
                e1.printStackTrace();
            }
            if(httpRetrieveResponse != null) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In DownloaderSlaveActor: \n" + e.toString());
            }
            if ( asyncHttpClient != null)
                asyncHttpClient.close();

        }
    }


}
