package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.pattern.CircuitBreaker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.processing.ProcessingResultTuple;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessor;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.utils.FileUtils;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This type of actors checks for a link or downloads a document.
 */
public class ProcessorActor extends UntypedActor {

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


    private final SlaveProcessor slaveProcessor;


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

        this.slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(colorMapPath), new ThumbnailGenerator(colorMapPath), new ColorExtractor(colorMapPath), mediaStorageClient, LOG);

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

        ProcessorHelperDownload downloader = new ProcessorHelperDownload(LOG);

        final HttpRetrieveResponse httpRetrieveResponse = downloader.downloadTask(task, pathToSave);

        DoneDownload doneDownload;

        if (httpRetrieveResponse.getHttpResponseCode() == -1)
            doneDownload = createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.ERROR);
        else
            doneDownload = createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.SUCCESS);


        sender.tell(doneDownload, getSelf());

        context.stop();

        if ((DocumentReferenceTaskType.CHECK_LINK).equals(doneDownload.getDocumentReferenceTask().getTaskType()) ||
                (ProcessingState.ERROR).equals(doneDownload.getProcessingState())) {

            deleteFile(doneDownload, httpRetrieveResponse.getAbsolutePath());
            LOG.info("Done download for task ID {}, send done message to sender", doneDownload.getTaskID());
            getSender().tell(new DoneProcessing(doneDownload, null, null, null, null), getSelf());
            getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());

        } else {

            LOG.info("Done download for task ID {}, start metadata processing", doneDownload.getTaskID());


            final Timer.Context context2 = dresponses.time();


            path = doneDownload.getHttpRetrieveResponse().getAbsolutePath();
            if (path.equals("")) {
                path = FileUtils.createFileAndFolderIfMissing(LOG, tmpFolder, doneDownload.getReferenceId(), doneDownload.getHttpRetrieveResponse().getContent());
            }
            error = "";

            final ProcessingJobTaskDocumentReference tsk = doneDownload.getDocumentReferenceTask();


            DoneProcessing doneProcessing;

            try {
                final ProcessingResultTuple processingResult = slaveProcessor.process(tsk,  path, doneDownload.getUrl(),doneDownload.getHttpRetrieveResponse().getContent(), responseType, source);

                doneProcessing = new DoneProcessing(doneDownload,
                        processingResult.getMediaMetaInfoTuple().getImageMetaInfo(),
                        processingResult.getMediaMetaInfoTuple().getAudioMetaInfo(),
                        processingResult.getMediaMetaInfoTuple().getVideoMetaInfo(),
                        processingResult.getMediaMetaInfoTuple().getTextMetaInfo());

            } catch (Exception e) {
                LOG.error("Exception while processing {}", e.getMessage());
                doneProcessing = new DoneProcessing(doneDownload, null, null, null, null);
            }

            context2.stop();

            getSender().tell(doneProcessing, getSelf());
            getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());

        }
    }



    /**
     * Deletes a file.
     */
    private void deleteFile(DoneDownload doneDownload, String path) {
        if (!doneDownload.getHttpRetrieveResponse().getAbsolutePath().equals("")) {
            return;
        }

        final File file = new File(path);
        if (file.exists())
            file.delete();
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





}
