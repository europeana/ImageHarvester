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
import eu.europeana.harvester.cluster.slave.processing.exceptiions.LocaleException;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;


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

    public static final ActorRef createActor(final ActorSystem system,
                                             final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                                             final SlaveProcessor processor
    ) {
        return system.actorOf(Props.create(RetrieveAndProcessActor.class,
                httpRetrieveResponseFactory, processor
        ));
    }

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

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
        this.slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(colorMapPath),
                new ThumbnailGenerator(colorMapPath),
                new ColorExtractor(colorMapPath),
                mediaStorageClient
        );
        this.slaveDownloader = new SlaveDownloader();
        this.slaveLinkChecker = new SlaveLinkChecker();
    }

    public RetrieveAndProcessActor(final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                                   final SlaveProcessor slaveProcessor) {

        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.slaveProcessor = slaveProcessor;
        this.slaveDownloader = new SlaveDownloader();
        this.slaveLinkChecker = new SlaveLinkChecker();
    }


    public void notifyMeOnOpen() {
        LOG.warn(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_PROCESSING, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                "The slave processing circuit breaker is now open, and will not close for one minute. Slave worker suicides now with poison pill.");
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

            if (response != null) {
                doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(),
                                                       response.getState(),
                        response, task.getDocumentReferenceTask(), task.getIpAddress());
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                        "Retrieval url finished with success and the temporary file is stored on disk at {}", taskWithProcessingConfig.getDownloadPath());
            } else {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                        "Retrieval url failed in an unexpected way. This might be a bug in harvester slave code.");
            }

        } catch (Exception e) {
            doneDownloadMessage = new DoneDownload(task.getId(),
                                                   task.getUrl(),
                                                   task.getReferenceId(),
                                                   task.getJobId(),
                                                   RetrievingState.ERROR,
                                                   response,
                                                   task.getDocumentReferenceTask(),
                                                   task.getIpAddress());
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                    "Exception during retrieval. The http retrieve response could not be created. Probable cause : wrong configuration argument in the slave.", e);
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

                if (processingResultTuple == null)
                    throw new IllegalStateException("Unexpected processingResultTuple with value null. Probable cause : bug in slave code.");
                if (processingResultTuple.getMediaMetaInfoTuple() == null) {
                    doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                                                               processingResultTuple.getProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.SUCCESS),
                                                                null, null, null, null
                                                              );
                    LOG.warn(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                              "The current job has not metainfo attached to it."
                            );
                }
                else {
                    doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                                                               processingResultTuple.getProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.SUCCESS),
                                                               processingResultTuple.getMediaMetaInfoTuple().getImageMetaInfo(),
                                                               processingResultTuple.getMediaMetaInfoTuple().getAudioMetaInfo(),
                                                               processingResultTuple.getMediaMetaInfoTuple().getVideoMetaInfo(),
                                                               processingResultTuple.getMediaMetaInfoTuple().getTextMetaInfo()
                                                             );
                }

            }
            catch (IOException | URISyntaxException e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_PROCESSING, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                          "Exception during processing. :  " + e.getLocalizedMessage(), e);

                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                                                           ProcessingJobSubTaskStats.withRetrievelSuccess(),
                                                           null, null, null, null,
                                                           e.getMessage());
            } finally {
                processingTimerContext.stop();
            }
        } else {
            // We can skip processing altogether.
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_PROCESSING, task.getJobId(), task.getUrl(), task.getReferenceOwner()),
                    "Processing stage skipped because retrieval involved only link checking or finished with non-complete state : " + response.getState() + " and reason " + response.getLog());

            if (response.getState() != RetrievingState.COMPLETED) {
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.ERROR),
                        null, null, null, null
                );
            }
            else {
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.SUCCESS),
                        null, null, null, null
                );
            }
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
                    response.setLoggingAppFields(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()));
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
                    response.setLoggingAppFields(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()));
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
                    response.setLoggingAppFields(LoggingComponent.appendAppFields(LoggingComponent.Slave.SLAVE_RETRIEVAL, task.getJobId(), task.getUrl(), task.getReferenceOwner()));
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
    private final ProcessingResultTuple executeProcessing(final HttpRetrieveResponse response, final RetrieveUrl task) throws
                                                                                                                       LocaleException,
                                                                                                                       URISyntaxException,
                                                                                                                       IOException {
        return slaveProcessor.process(task.getDocumentReferenceTask(),
                                      taskWithProcessingConfig.getDownloadPath(),
                                      response.getUrl().toURI().toASCIIString(),
                                      response.getContent(),
                                      responseTypeFromTaskType(task.getDocumentReferenceTask().getTaskType()),
                                      task.getReferenceOwner()
        );
    }

    private long computeMaximumRetrievalAndProcessingDurationInMinutes(RetrieveUrl retrieveUrl) {
        long duration;
        try {
            duration = Duration.create(retrieveUrl.getLimits().getRetrievalTerminationThresholdTimeLimitInMillis() + retrieveUrl.getLimits().getProcessingTerminationThresholdTimeLimitInMillis(), TimeUnit.MILLISECONDS).toMinutes();
        } catch (Exception e) {
            ProcessingJobLimits lm = new ProcessingJobLimits();
            duration = Duration.create(lm.getRetrievalTerminationThresholdTimeLimitInMillis() + lm.getProcessingTerminationThresholdTimeLimitInMillis(), TimeUnit.MILLISECONDS).toMinutes();
        }
        return duration;
    }

}
