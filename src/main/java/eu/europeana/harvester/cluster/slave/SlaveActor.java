package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.UntypedActorWithStash;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.httpclient.HttpClient;
import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.net.URL;

public class SlaveActor extends UntypedActorWithStash {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    private final HttpRetrieveResponseFactory httpRetrieveResponseFactory;

    private final ResponseType responseType;

    private final String pathToSave;

    public SlaveActor(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer,
                      HttpRetrieveResponseFactory httpRetrieveResponseFactory, ResponseType responseType,
                      String pathToSave) {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
        this.httpRetrieveResponseFactory = httpRetrieveResponseFactory;
        this.responseType = responseType;
        this.pathToSave = pathToSave;
        log.info("Create");
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            final ActorRef sender = getSender();
            final RetrieveUrl task = (RetrieveUrl) message;

            final StartedUrl startedUrl = new StartedUrl(task.getUrl());
            sender.tell(startedUrl, getSelf());

            HttpRetrieveResponse httpRetrieveResponse =
                    httpRetrieveResponseFactory.create(responseType, pathToSave, new URL(task.getUrl()));

            final HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer,
                    task.getHttpRetrieveConfig(), httpRetrieveResponse, HttpGET.build(new URL(task.getUrl())));

            httpRetrieveResponse = httpClient.call();

            sender.tell(new SendResponse(httpRetrieveResponse, task.getJobId()), getSelf());
        }
    }
}
