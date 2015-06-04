package eu.europeana.harvester.cluster.slave;

import akka.actor.*;
import akka.pattern.CircuitBreaker;
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
import eu.europeana.harvester.domain.ProcessingJobLimits;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static net.logstash.logback.marker.Markers.append;

/**
 * This actor is the actual worker actor.
 * It does retrieval & processing, wrapping the dangerous activity of executing a job.
 */
public class RetrieveAndProcessActor extends UntypedActor {

    public static final ActorRef createActor(final ActorSystem system,
                                             final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                                             final MediaStorageClient mediaStorageClient,
                                             final String colorMapPath
    ) {
        return system.actorOf(Props.create(RetrieveAndProcessActor.class,
                httpRetrieveResponseFactory, colorMapPath, mediaStorageClient
        ));
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

    private final SlaveProcessor slaveProcessor;

    private final SlaveDownloader slaveDownloader;

    private final SlaveLinkChecker slaveLinkChecker;

    public RetrieveAndProcessActor(final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                                   final String colorMapPath,
                                   final MediaStorageClient mediaStorageClient
    ) {

        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(colorMapPath), new ThumbnailGenerator(colorMapPath), new ColorExtractor(colorMapPath), mediaStorageClient);
        this.slaveDownloader = new SlaveDownloader();
        this.slaveLinkChecker = new SlaveLinkChecker();
    }


    public void notifyMeOnOpen() {
        LOG.info(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, "Circuit Breaker"), "The slave processing circuit breaker is now open, and will not close for one minute. Killing current actor.");
        SlaveMetrics.Worker.Slave.forcedSelfDestructCounter.inc();
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

        final Timer.Context downloadTimerContext = SlaveMetrics.Worker.Slave.Retrieve.totalDuration.time();

        try {

            response = executeRetrieval(task);

            doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), (response.getState() == RetrievingState.COMPLETED) ? ProcessingState.SUCCESS : ProcessingState.ERROR,
                    response, task.getDocumentReferenceTask(), task.getIpAddress());
            LOG.info(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()), "Retrieval of {} finished and the temporary file is stored on disk at {}", task.getUrl(), taskWithProcessingConfig.getDownloadPath());

        } catch (Exception e) {
            doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), ProcessingState.ERROR,
                    response, task.getDocumentReferenceTask(), task.getIpAddress());

            LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()), "Exception during retrieval. The http retrieve response could not be created. Probable cause : wrong configuration argument in the slave. The actual message : {}", e.getMessage());
        } finally {
            downloadTimerContext.stop();
            sender.tell(doneDownloadMessage, getSelf());
        }

        // Step 2 : Execute processing + send confirmation when it's done
        DoneProcessing doneProcessingMessage = null;

        if (response.getState() == RetrievingState.COMPLETED && task.getDocumentReferenceTask().getTaskType() != DocumentReferenceTaskType.CHECK_LINK) {
            final Timer.Context processingTimerContext = SlaveMetrics.Worker.Slave.Processing.totalDuration.time();
            ProcessingResultTuple processingResultTuple;
            try {
                processingResultTuple = executeProcessing(response, task);
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        processingResultTuple.getMediaMetaInfoTuple().getImageMetaInfo(),
                        processingResultTuple.getMediaMetaInfoTuple().getAudioMetaInfo(),
                        processingResultTuple.getMediaMetaInfoTuple().getVideoMetaInfo(),
                        processingResultTuple.getMediaMetaInfoTuple().getTextMetaInfo());

            } catch (Exception e) {
                LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()), "Exception during processing. The actual message : {}", e.getMessage());
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        null,
                        null,
                        null,
                        null, e.getMessage());
            } finally {
                processingTimerContext.stop();
            }
        } else {
            // We can skip processing altogether.
            LOG.error(append(LogMarker.EUROPEANA_PROCESSING_JOB_ID, task.getJobId()), "The task processing stage was skipped because the retrieval stage failed.");
            LOG.info("response state is {}, response log {}", response.getState(), response.getLog());
            doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                    null,
                    null,
                    null,
                    null);
        }

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
                SlaveMetrics.Worker.Slave.Retrieve.linkCheckingCounter.inc();
                final Timer.Context downloadLinkCheckingTimerContext = SlaveMetrics.Worker.Slave.Retrieve.linkCheckingDuration.time();
                try {
                    response = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE, taskWithProcessingConfig.getDownloadPath());
                    slaveLinkChecker.downloadAndStoreInHttpRetrievResponse(response, task);
                } finally {
                    downloadLinkCheckingTimerContext.stop();
                }
                break;
            case UNCONDITIONAL_DOWNLOAD:
                SlaveMetrics.Worker.Slave.Retrieve.unconditionalDownloadCounter.inc();
                final Timer.Context downloadUnconditionalDownloadTimerContext = SlaveMetrics.Worker.Slave.Retrieve.unconditionalDownloadDuration.time();
                try {
                    response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, taskWithProcessingConfig.getDownloadPath());
                    slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);
                } finally {
                    downloadUnconditionalDownloadTimerContext.stop();
                }
                break;
            case CONDITIONAL_DOWNLOAD:
                SlaveMetrics.Worker.Slave.Retrieve.conditionalDownloadCounter.inc();
                final Timer.Context downloadConditionalDownloadTimerContext = SlaveMetrics.Worker.Slave.Retrieve.conditionalDownloadDuration.time();

                try {
                    response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, taskWithProcessingConfig.getDownloadPath());
                    slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);
                } finally {
                    downloadConditionalDownloadTimerContext.stop();
                }

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
        long duration;
        try {
            duration = Duration.create(retrieveUrl.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis() + retrieveUrl.getLimits().getProcessingTerminationThresholdTimeLimitInMillis(), TimeUnit.MILLISECONDS).toMinutes();
        } catch ( Exception e) {
            ProcessingJobLimits lm = new ProcessingJobLimits();
            duration = Duration.create(lm.getRetrievalTerminationThresholdTimeLimitInMillis() + lm.getProcessingTerminationThresholdTimeLimitInMillis(), TimeUnit.MILLISECONDS).toMinutes();
        }
        return duration;
    }

}
