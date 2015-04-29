package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.HttpClient;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.utils.CallbackInterface;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * This type of actors checks for a link or downloads a document.
 */
public class DownloaderSlaveActor extends UntypedActor implements CallbackInterface {

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
     * Response type: memory or disk storage.
     */
    private final ResponseType responseType;

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
    private final ExecutorService executorServiceAkka;

    private Future future;

    private final MetricRegistry metrics;
    private Timer responses ;


    public DownloaderSlaveActor(final ChannelFactory channelFactory, final HashedWheelTimer hashedWheelTimer,
                                final HttpRetrieveResponseFactory httpRetrieveResponseFactory, final ResponseType responseType,
                                final String pathToSave, final ExecutorService executorServiceAkka, MetricRegistry metrics) {
        LOG.info("DownloaderSlaveActor constructor");

        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.responseType = responseType;
        this.pathToSave = pathToSave;
        this.executorServiceAkka = executorServiceAkka;
        this.metrics = metrics;


        responses = metrics.timer(name(DownloaderSlaveActor.class, "Download responses"));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        sender = getSender();

        if(message instanceof RetrieveUrl) {
            task = (RetrieveUrl) message;
            final Timer.Context context = responses.time();

            //LOG.info("Starting download for task ID {}", task.getId());

            final HttpRetrieveResponse httpRetrieveResponse = downloadTask(task);
            if(httpRetrieveResponse.getHttpResponseCode() == -1) {
                final DoneDownload doneDownload =
                        createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.ERROR);

                sender.tell(doneDownload, getSelf());
            }
            context.stop();

            return;
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

            final HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer,
                    task.getHttpRetrieveConfig(), task.getHeaders(), httpRetrieveResponse, url);
            httpClient.setMaster(this);
            if(httpRetrieveResponse.getHttpResponseCode() == -1) {
                return httpRetrieveResponse;
            }

            future = executorServiceAkka.submit(httpClient);

            // blocks until we get the response
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
            //LOG.info("Done download for task ID {} with error", doneDownload.getTaskID());

            future.cancel(true);
            sender.tell(doneDownload, getSelf());

            return;
        }

        final DoneDownload doneDownload =
                createDoneDownloadMessage(httpRetrieveResponse, ProcessingState.SUCCESS);

        //LOG.info("Done download for task ID {} with success", doneDownload.getTaskID());

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

}
