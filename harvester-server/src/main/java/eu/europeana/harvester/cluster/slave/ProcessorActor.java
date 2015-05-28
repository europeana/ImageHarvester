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
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.cluster.slave.downloading.SlaveLinkChecker;
import eu.europeana.harvester.cluster.slave.processing.ProcessingResultTuple;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessor;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import scala.concurrent.duration.Duration;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This type of actors checks for a link or downloads a document.
 */
public class ProcessorActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

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

    private final MetricRegistry metrics;
    private final Timer responses, dresponses;

    /**
     * If the response type is disk storage then the absolute path on disk where
     * the content of the download will be saved.
     */
    private String path;

    private final String source;

    private final SlaveProcessor slaveProcessor;

    private final SlaveDownloader slaveDownloader;

    private final SlaveLinkChecker slaveLinkChecker;

    public ProcessorActor(final HttpRetrieveResponseFactory httpRetrieveResponseFactory,
                          final SlaveDownloader slaveDownloader,
                          final SlaveLinkChecker slaveLinkChecker,
                          final SlaveProcessor slaveProcessor,
                          String source, MetricRegistry metrics) {

        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.source = source;
        this.metrics = metrics;
        this.slaveProcessor = slaveProcessor;
        this.slaveDownloader = slaveDownloader;
        this.slaveLinkChecker = slaveLinkChecker;

        responses = metrics.timer(name("ProcessorSlave", "Download responses"));
        dresponses = metrics.timer(name("ProcessorSlave", "Process responses"));

    }


    public void notifyMeOnOpen() {
        LOG.warning("The slave processing circuit breaker is now open, and will not close for one minute. Killing current actor.");
        getSelf().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    @Override
    public void onReceive(Object message) throws Exception {
        sender = getSender();

        if (message instanceof RetrieveUrl) {
            task = (RetrieveUrl) message;

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
        try {
            response = executeRetrieval(task);
            doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), (response.getState() == ResponseState.COMPLETED) ? ProcessingState.SUCCESS : ProcessingState.ERROR,
                    response, task.getDocumentReferenceTask(), task.getIpAddress());

        } catch (Exception e) {
            doneDownloadMessage = new DoneDownload(task.getId(), task.getUrl(), task.getReferenceId(), task.getJobId(), ProcessingState.ERROR,
                    response, task.getDocumentReferenceTask(), task.getIpAddress());

            LOG.error("Exception during retrieval. The http retrieve response could not be created. Probable cause : wrong configuration argument in the slave. The actual message : {}", e.getMessage());
        } finally {
            sender.tell(doneDownloadMessage, getSelf());
        }

        // Step 2 : Execute processing + send confirmation when it's done
        DoneProcessing doneProcessingMessage = null;
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
                LOG.error("Exception during processing. The actual message : {}", e.getMessage());
                doneProcessingMessage = new DoneProcessing(doneDownloadMessage,
                        null,
                        null,
                        null,
                        null, e.getMessage());
            }
        } else {
            // We can skip processing altogether.
            LOG.error("The task processing stage was skipped because the retrieval stage failed.");
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
     * @param task
     * @return
     * @throws Exception
     */
    private final HttpRetrieveResponse executeRetrieval(final RetrieveUrl task) throws Exception {
        HttpRetrieveResponse response = null;
        switch (task.getDocumentReferenceTask().getTaskType()) {
            case CHECK_LINK:
                response = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE, null);
                slaveLinkChecker.downloadAndStoreInHttpRetrievResponse(response, task);
                break;
            case UNCONDITIONAL_DOWNLOAD:
                response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, path);
                slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);
                break;
            case CONDITIONAL_DOWNLOAD:
                response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, path);
                slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);
                break;
            default:
                throw new IllegalArgumentException("Cannot create http response when preparing processing for unknown task type " + task.getDocumentReferenceTask().getTaskType());
        }
        return response;
    }

    private ResponseType responseTypeFromTaskType(final DocumentReferenceTaskType taskType) {
        switch (taskType) {
            case CHECK_LINK :
                return ResponseType.NO_STORAGE;
            case UNCONDITIONAL_DOWNLOAD:
                return ResponseType.DISK_STORAGE;
            case CONDITIONAL_DOWNLOAD:
                return ResponseType.DISK_STORAGE;
            default :
                throw new IllegalArgumentException("Cannot convert task type to response type. Unknown task type "+taskType.name());
        }
    }

    /**
     * Executes the processing phase.
     * @param response
     * @param task
     * @return
     * @throws Exception
     */
    private final ProcessingResultTuple executeProcessing(final HttpRetrieveResponse response, final RetrieveUrl task) throws Exception {
        return slaveProcessor.process(task.getDocumentReferenceTask(), path, response.getUrl().toURI().toASCIIString(), response.getContent(),responseTypeFromTaskType(task.getDocumentReferenceTask().getTaskType()) , source);
    }

    private long computeMaximumRetrievalAndProcessingDurationInMinutes(RetrieveUrl retrieveUrl) {
        return retrieveUrl.getHttpRetrieveConfig().getTerminationThresholdTimeLimit().getStandardMinutes()+10;
    }

}
