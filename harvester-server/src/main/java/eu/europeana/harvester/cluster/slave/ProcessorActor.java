package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.CircuitBreaker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.ning.http.client.*;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.utils.CallbackInterface;
import eu.europeana.harvester.utils.FileUtils;
import eu.europeana.harvester.utils.MediaMetaDataUtils;
import eu.europeana.harvester.utils.ThumbnailUtils;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This type of actors checks for a link or downloads a document.
 */
public class ProcessorActor extends UntypedActor implements CallbackInterface {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    /**
     * A factory object which build different types of httpRetrieveResponse objects.
     */
    private final HttpRetrieveResponseFactory httpRetrieveResponseFactory;


    /**
     * If the response type is disk storage then the absolute path on disk where
     * the content of the download will be saved.
     */
    private final String pathToSave;

    /**
     * An actor reference to the sender of the message.
     */
    private ActorRef sender;

    /**
     * An object which represents a task.
     */
    private RetrieveUrl task;

    /**
     * Executor service for assync method calls.
     */
    //private final ExecutorService executorServiceAkka;

    private Future future;

    private final MetricRegistry metrics;
    private Timer responses, dresponses ;

    //Processing-related fields

    private final static File tmpFolder = new File("/tmp/europeana");


    /**
     * If the response type is disk storage then the absolute path on disk where
     * the content of the download will be saved.
     */
    private String path;

    /**
     * Response type: memory or disk storage.
     */
    private final ResponseType responseType;

    /**
     * The response from the downloader actor.
     */
    //private DoneDownload doneDownload;

    /**
     * The error message if any error occurs.
     */
    private String error;

    /**
     * The module which stores the thumbnails in MongoDB.
     */
    private final MediaStorageClient mediaStorageClient;

    private final String source;

    /**
     * Path to a file needed for the colormap extraction.
     */
    private final String colorMapPath;

    /**
     * A list of created thumbnails.
     */
    private final List<MediaFile> thumbnails = new ArrayList<>();




    private final CircuitBreaker breaker;

    public ProcessorActor(final ChannelFactory channelFactory, final HashedWheelTimer hashedWheelTimer,
                          final HttpRetrieveResponseFactory httpRetrieveResponseFactory, final ResponseType responseType,
                          final String pathToSave, final ExecutorService executorServiceAkka, MediaStorageClient mediaStorageClient,
                          String source, String colorMapPath, MetricRegistry metrics) {


        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.responseType = responseType;
        this.pathToSave = pathToSave;
        this.mediaStorageClient = mediaStorageClient;
        this.source = source;
        this.colorMapPath = colorMapPath;
        //this.executorServiceAkka = Executors.newSingleThreadExecutor();
        this.metrics = metrics;


        responses = metrics.timer(name("ProcessorSlave", "Download responses"));
        dresponses = metrics.timer(name("ProcessorSlave", "Process responses"));
        breaker = new CircuitBreaker(
                getContext().dispatcher(), getContext().system().scheduler(),
                5, Duration.create(10,TimeUnit.MINUTES), Duration.create(1, TimeUnit.MINUTES))
                .onOpen(new Runnable() {
                    public void run() {
                        notifyMeOnOpen();
                    }
                });

    }


    public void notifyMeOnOpen() {
        LOG.warning("My CircuitBreaker is now open, and will not close for one minute");
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());

    }

    @Override
    public void onReceive(Object message) throws Exception {
        sender = getSender();

        if(message instanceof RetrieveUrl) {
            task = (RetrieveUrl) message;

            breaker.callWithSyncCircuitBreaker(
                    new Callable() {
                        @Override
                        public Object call() throws Exception {
                            process(task);
                            return null;
                        }
                    }
            );

            return;
        }
    }


    private void process( RetrieveUrl task ) {


        LOG.info("Starting processing for task ID {}", task.getId());

        final Timer.Context context = responses.time();

        final HttpRetrieveResponse httpRetrieveResponse = downloadTask(task);

        DoneDownload doneDownload;

        if (httpRetrieveResponse.getHttpResponseCode() == -1)
            doneDownload = createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.ERROR);
        else
            doneDownload = createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.SUCCESS);


        sender.tell(doneDownload, getSelf());

        context.stop();

        if ((DocumentReferenceTaskType.CHECK_LINK).equals(doneDownload.getDocumentReferenceTask().getTaskType()) ||
                (ProcessingState.ERROR).equals(doneDownload.getProcessingState())) {

            deleteFile(doneDownload);
            LOG.info("Done download for task ID {}, send done message to sender", doneDownload.getTaskID());
            getSender().tell(new DoneProcessing(doneDownload, null, null, null, null), getSelf());
            getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());

        } else {


            final Timer.Context context2 = dresponses.time();


            path = doneDownload.getHttpRetrieveResponse().getAbsolutePath();
            if (path.equals("")) {
                path = FileUtils.createFileAndFolderIfMissing(LOG, tmpFolder, doneDownload.getReferenceId(), doneDownload.getHttpRetrieveResponse().getContent());
            }
            error = "";

            final ProcessingJobTaskDocumentReference tsk = doneDownload.getDocumentReferenceTask();
            LOG.info("Done download for task ID {}, start processing", doneDownload.getTaskID());

            try {
                startProcessing(tsk, doneDownload);
            } catch (Exception e) {
                LOG.error("Exception in startProcessing {}", e.getMessage());
            }

            context2.stop();
        }
    }

    /**
     * Starts one download
     * @param task contains all the information for this task
     * @return - finished job response with all the collected information
     */
    private HttpRetrieveResponse downloadTask(RetrieveUrl task) {
        HttpRetrieveResponse httpRetrieveResponse = null;
        final String path = pathToSave + "/" + task.getReferenceId();

        try {


            if((DocumentReferenceTaskType.CHECK_LINK).equals(task.getHttpRetrieveConfig().getTaskType())) {
                httpRetrieveResponse = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE, path);
            } else {
                httpRetrieveResponse = httpRetrieveResponseFactory.create(responseType, path);
            }

            final String url = task.getUrl();


            try {
                httpRetrieveResponse.setUrl(new URL(url));
            } catch (MalformedURLException e) {
                httpRetrieveResponse.setHttpResponseCode(-1);
                httpRetrieveResponse.setLog("In DownloaderSlaveActor: \n" + e.toString());
                return httpRetrieveResponse;
            }

            download(httpRetrieveResponse);

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

    @Override
    public void returnResult(Object result) {
        final HttpRetrieveResponse httpRetrieveResponse = (HttpRetrieveResponse)result;

        if(httpRetrieveResponse.getHttpResponseCode() == -1) {
            final DoneDownload doneDownload =
                    createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.ERROR);
            LOG.info("Done download for task ID {} with error", doneDownload.getTaskID());

            future.cancel(true);
            sender.tell(doneDownload, getSelf());

            return;
        }

        final DoneDownload doneDownload =
                createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.SUCCESS);

        LOG.info("Done download for task ID {} with success", doneDownload.getTaskID());

        future.cancel(true);
        sender.tell(doneDownload, getSelf());
    }

    /**
     * Creates the response message from the result of the task.
     * @param httpRetrieveResponse response from HttpClient
     * @param processingState the state of the task
     * @return - the response message
     */
    private DoneDownload createDoneDownloadMessage(HttpRetrieveResponse httpRetrieveResponse,
                                                   ProcessingState processingState) {

        ProcessingState finalProcessingState = processingState;
        if((ResponseState.ERROR).equals(httpRetrieveResponse.getState())) { //||
                //httpRetrieveResponse.getState().equals(ResponseState.PREPARING) ||
                //httpRetrieveResponse.getState().equals(ResponseState.PROCESSING)) {
            finalProcessingState = ProcessingState.ERROR;
        }

        return new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), finalProcessingState,
                httpRetrieveResponse, task.getDocumentReferenceTask(), task.getIpAddress());
    }


    private void download ( final HttpRetrieveResponse httpRetrieveResponse ) {
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

                    return STATE.CONTINUE;
                }

                @Override
                public STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
                    httpRetrieveResponse.addContent(bodyPart.getBodyPartBytes());
                    return STATE.CONTINUE;
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



    private void startProcessing(final ProcessingJobTaskDocumentReference task, DoneDownload doneDownload) throws Exception {
        final List<ProcessingJobSubTask> subTasks = task.getProcessingTasks();

        DoneProcessing doneProcessing = null;
        Boolean isThumbnail = false;
        ImageMetaInfo metaInfoForThumbnails = null;

        try {

            for (ProcessingJobSubTask subTask : subTasks) {
                switch (subTask.getTaskType()) {
                    case META_EXTRACTION:
                        doneProcessing = metadataExtraction(doneDownload);

                        break;
                    case GENERATE_THUMBNAIL:
                        final GenericSubTaskConfiguration config = subTask.getConfig();
                        generateThumbnail(config, doneDownload);

                        break;
                    case COLOR_EXTRACTION:
                        metaInfoForThumbnails = MediaMetaDataUtils.colorExtraction(path, colorMapPath);
                        isThumbnail = true;
                        if (metaInfoForThumbnails == null) {
                            doneProcessing = new DoneProcessing(doneDownload, null, null, null, null);
                            break;
                        }

                        if (doneProcessing != null) {
                            doneProcessing = doneProcessing.withColorPalette(metaInfoForThumbnails);
                        } else {
                            doneProcessing = new DoneProcessing(doneDownload, metaInfoForThumbnails, null, null, null);
                        }

                        break;
                    default:
                        LOG.info("Unknown subtask in job: {}, referenceId: {}", doneDownload.getJobId(),
                                doneDownload.getReferenceId());
                }
            }

            if (isThumbnail) {
                for (MediaFile thumbnail : thumbnails) {
                    if (metaInfoForThumbnails != null) {
                        final MediaFile newThumbnail = thumbnail.withMetaInfo(metaInfoForThumbnails.getColorPalette());
                        mediaStorageClient.createOrModify(newThumbnail);
                    } else {
                        mediaStorageClient.createOrModify(thumbnail);
                    }
                }
                thumbnails.clear();
            }
            deleteFile(doneDownload);
        } catch (Exception e) {
            LOG.info("Error in startProcessing : {}",e.getMessage());
        }

        if (error.length() != 0 && doneProcessing != null) {
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, error);
        }
        if (doneProcessing == null) {
            doneProcessing = new DoneProcessing(doneDownload, null, null, null, null);
            doneProcessing = doneProcessing.withNewState(ProcessingState.ERROR, "Error in processing");
        }

        //deleteFile();
        LOG.info("Done processing for task ID {}, telling the master ", doneProcessing.getTaskID());
        getSender().tell(doneProcessing, getSelf());
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    /**
     * Deletes a file.
     */
    private void deleteFile(DoneDownload doneDownload) {
        if (!doneDownload.getHttpRetrieveResponse().getAbsolutePath().equals("")) {
            return;
        }

        final File file = new File(path);
        file.delete();
    }

    /**
     * Extracts all the metadata from a file.
     *
     * @return an object with metadata and additional information about the processing.
     * @throws InterruptedException
     */
    private DoneProcessing metadataExtraction(DoneDownload doneDownload) throws InterruptedException {
        return extract(doneDownload);
    }

    /**
     * Generates thumbnail from an image.
     *
     * @param config
     * @throws InterruptedException
     */
    private void generateThumbnail(GenericSubTaskConfiguration config, DoneDownload doneDownload) throws Exception {
        if (MediaMetaDataUtils.classifyUrl(path).equals(ContentType.IMAGE)) {
            thumbnails.add(ThumbnailUtils.createMediaFileWithThumbnail(config.getThumbnailConfig(), source, doneDownload.getUrl(), doneDownload.getHttpRetrieveResponse().getContent(), path, colorMapPath));
        }
    }


    /**
     * Classifies the document and performs the specific operations for this actor.
     *
     * @return - the response message.
     */
    private DoneProcessing extract(DoneDownload doneDownload) {
        final ContentType contentType = MediaMetaDataUtils.classifyUrl(path);

        ImageMetaInfo imageMetaInfo = null;
        AudioMetaInfo audioMetaInfo = null;
        VideoMetaInfo videoMetaInfo = null;
        TextMetaInfo textMetaInfo = null;
        try {
            if (!responseType.equals(ResponseType.NO_STORAGE)) {
                switch (contentType) {
                    case TEXT:
                        textMetaInfo = MediaMetaDataUtils.extractTextMetaData(path);
                        break;
                    case IMAGE:
                        imageMetaInfo = MediaMetaDataUtils.extractImageMetadata(path, colorMapPath);
                        break;
                    case VIDEO:
                        videoMetaInfo = MediaMetaDataUtils.extractVideoMetaData(path);
                        break;
                    case AUDIO:
                        audioMetaInfo = MediaMetaDataUtils.extractAudioMetadata(path);
                        break;
                    case UNKNOWN:
                        break;
                }
            }
            return new DoneProcessing(doneDownload, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo);
        } catch (Exception e) {
            LOG.info("Error in Processing slave - extract : {}",e.getMessage());
            return new DoneProcessing(doneDownload, imageMetaInfo, audioMetaInfo, videoMetaInfo, textMetaInfo).withNewState(ProcessingState.ERROR,error);
        }

    }


}
