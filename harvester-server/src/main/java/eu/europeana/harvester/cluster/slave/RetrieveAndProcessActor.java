package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
import akka.pattern.CircuitBreaker;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrlWithProcessingConfig;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.cluster.slave.downloading.SlaveLinkChecker;
import eu.europeana.harvester.cluster.slave.processing.ProcessingResultTuple;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessor;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.LogMarker;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;
import static net.logstash.logback.marker.Markers.*;

/**
 * This actor is the actual worker actor.
 * It does retrieval & processing, wrapping the dangerous activity of executing a job.
 */
public class RetrieveAndProcessActor extends UntypedActor {

    public static final ActorRef createActor(final ActorSystem system,
                                             final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                                             final MediaStorageClient mediaStorageClient,
                                             final String colorMapPath,
                                             MetricRegistry metrics) {
        return system.actorOf(Props.create(RetrieveAndProcessActor.class,
                httpRetrieveResponseFactory, colorMapPath, mediaStorageClient,
                metrics));
    }

    //private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);
    Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * A factory object which build different types of httpRetrieveResponse objects.
     */
    private final HttpRetrieveResponseFactory httpRetrieveResponseFactory;

    /**
     * An actor reference to the sender of the message.
     */
    private ActorRef sender;

    /**
     * An object which represents a task.
     */
    private RetrieveUrl task;

    private RetrieveUrlWithProcessingConfig taskWithProcessingConfig;

    private final MetricRegistry metrics;

    private final Timer responses, dresponses;

    private final SlaveProcessor slaveProcessor;

    private final SlaveDownloader slaveDownloader;

    private final SlaveLinkChecker slaveLinkChecker;

    public RetrieveAndProcessActor(final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                                   final String colorMapPath,
                                   final MediaStorageClient mediaStorageClient,
                                   final MetricRegistry metrics) {

        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.metrics = metrics;
        this.slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(colorMapPath),new ThumbnailGenerator(colorMapPath), new ColorExtractor(colorMapPath),mediaStorageClient);
        this.slaveDownloader = new SlaveDownloader();
        this.slaveLinkChecker = new SlaveLinkChecker();

        responses = metrics.timer(name("ProcessorSlave", "Download responses"));
        dresponses = metrics.timer(name("ProcessorSlave", "Process responses"));

    }


    public void notifyMeOnOpen() {
        LOG.info(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, "Circuit Breaker"),"The slave processing circuit breaker is now open, and will not close for one minute. Killing current actor.");
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        sender = getSender();

        if (message instanceof RetrieveUrlWithProcessingConfig) {
            taskWithProcessingConfig = (RetrieveUrlWithProcessingConfig) message;
            task = taskWithProcessingConfig.getRetrieveUrl();
            final CircuitBreaker breaker = new CircuitBreaker(
                    getContext().dispatcher(), getContext().system().scheduler(),
                    5, Duration.create(computeMaximumRetrievalAndProcessingDurationInMinutes(task), TimeUnit.MINUTES), Duration.create(1, TimeUnit.MINUTES))
                    .onOpen(new Runnable() {
                        public void run() {
                            notifyMeOnOpen();
                        }
                    });

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

    private void process(RetrieveUrl task) {

        DoneDownload doneDownloadMessage = null;
        HttpRetrieveResponse response = null;

        // Step 1 : Execute retrieval (download OR link checking) + send confirmation when it's done
        //System.out.println("Starting retrieval of "+ task.getUrl());

        final Timer.Context ctx = dresponses.time();

        try {

            response = executeRetrieval(task);

            doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), (response.getState() == ResponseState.COMPLETED) ? ProcessingState.SUCCESS : ProcessingState.ERROR,
                    response, task.getDocumentReferenceTask(), task.getIpAddress());
            LOG.info(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()),"Retrieval of {} finished and the temporary file is stored on disk at {}", task.getUrl(), taskWithProcessingConfig.getDownloadPath());

        } catch (Exception e) {
            doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), ProcessingState.ERROR,
                    response, task.getDocumentReferenceTask(), task.getIpAddress());

            LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()),"Exception during retrieval. The http retrieve response could not be created. Probable cause : wrong configuration argument in the slave. The actual message : {}", e.getMessage());
        } finally {
            sender.tell(doneDownloadMessage, getSelf());
        }
        ctx.stop();

        // Step 2 : Execute processing + send confirmation when it's done
        DoneProcessing doneProcessingMessage = null;
        final Timer.Context ctx2 = responses.time();

        if (response.getState() == ResponseState.COMPLETED && task.getDocumentReferenceTask().getTaskType() != DocumentReferenceTaskType.CHECK_LINK) {
            ProcessingResultTuple processingResultTuple;
            try {
                processingResultTuple = executeProcessing(response, task);
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        processingResultTuple.getMediaMetaInfoTuple().getImageMetaInfo(),
                        processingResultTuple.getMediaMetaInfoTuple().getAudioMetaInfo(),
                        processingResultTuple.getMediaMetaInfoTuple().getVideoMetaInfo(),
                        processingResultTuple.getMediaMetaInfoTuple().getTextMetaInfo());

            } catch (Exception e) {
                LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()),"Exception during processing. The actual message : {}", e.getMessage());
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        null,
                        null,
                        null,
                        null, e.getMessage());
            }
        } else {
            // We can skip processing altogether.
            LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()),"The task processing stage was skipped because the retrieval stage failed.");
            LOG.info("response state is {}, response log {}", response.getState(), response.getLog());
            doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                    null,
                    null,
                    null,
                    null);
        }
        ctx2.stop();

        sender.tell(doneProcessingMessage, getSelf());

        // Step 3 : Gentle suicide!
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());

    }


    /**
     * Executes the retrieval phase.
     *
     * @param task
     * @return
     * @throws Exception
     */
    private final HttpRetrieveResponse executeRetrieval(final RetrieveUrl task) throws Exception {
        HttpRetrieveResponse response = null;
        switch (task.getDocumentReferenceTask().getTaskType()) {
            case CHECK_LINK:
                response = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE, taskWithProcessingConfig.getDownloadPath());
                slaveLinkChecker.downloadAndStoreInHttpRetrievResponse(response, task);
                break;
            case UNCONDITIONAL_DOWNLOAD:
                try {
                    response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, taskWithProcessingConfig.getDownloadPath());
                    slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);
                } catch (Exception e) {
                    LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()),"The slave downloader for task type UNCONDITIONAL_DOWNLOAD for path {} has thrown an exception while processing {}",taskWithProcessingConfig.getDownloadPath(), e);
                    throw e;
                }
                break;
            case CONDITIONAL_DOWNLOAD:
                response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, taskWithProcessingConfig.getDownloadPath());
                slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);
                break;
            default:
                throw new IllegalArgumentException("Cannot create http response when preparing processing for unknown task type " + task.getDocumentReferenceTask().getTaskType());
        }
        return response;
    }

    private ResponseType responseTypeFromTaskType(final DocumentReferenceTaskType taskType) {
        switch (taskType) {
            case CHECK_LINK:
                return ResponseType.NO_STORAGE;
            case UNCONDITIONAL_DOWNLOAD:
                return ResponseType.DISK_STORAGE;
            case CONDITIONAL_DOWNLOAD:
                return ResponseType.DISK_STORAGE;
            default:
                throw new IllegalArgumentException("Cannot convert task type to response type. Unknown task type " + taskType.name());
        }
    }

    /**
     * Executes the processing phase.
     *
     * @param response
     * @param task
     * @return
     * @throws Exception
     */
    private final ProcessingResultTuple executeProcessing(final HttpRetrieveResponse response, final RetrieveUrl task) throws Exception {
        return slaveProcessor.process(task.getDocumentReferenceTask(), taskWithProcessingConfig.getDownloadPath(), response.getUrl().toURI().toASCIIString(), response.getContent(), responseTypeFromTaskType(task.getDocumentReferenceTask().getTaskType()),
                taskWithProcessingConfig.getProcessingSource());
    }

    private long computeMaximumRetrievalAndProcessingDurationInMinutes(RetrieveUrl retrieveUrl) {
        return Duration.create(retrieveUrl.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis()+retrieveUrl.getLimits().getProcessingTerminationThresholdTimeLimitInMillis(),TimeUnit.MILLISECONDS).toMinutes();
    }

}
