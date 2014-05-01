package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.UntypedActorWithStash;
import eu.europeana.harvester.cluster.messages.*;
import eu.europeana.harvester.httpclient.HttpClient;
import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseMemoryStorage;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.net.URL;

public class SlaveActor extends UntypedActorWithStash {

    /**
     * The channel factory used by netty to build the channel.
     */
    private final ChannelFactory channelFactory;

    /**
     * The wheel timer usually shared across all clients.
     */
    private final HashedWheelTimer hashedWheelTimer;

    public SlaveActor(ChannelFactory channelFactory, HashedWheelTimer hashedWheelTimer) {
        this.channelFactory = channelFactory;
        this.hashedWheelTimer = hashedWheelTimer;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof RetrieveUrl) {
            final ActorRef sender = getSender();
            final RetrieveUrl task = (RetrieveUrl) message;

            final StartedUrl startedUrl = new StartedUrl(task.getUrl());

            sender.tell(startedUrl, getSelf());

            HttpRetrieveResponse httpRetrieveResponse = new HttpRetrieveResponseMemoryStorage();
            final HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer,
                    task.getHttpRetrieveConfig(), httpRetrieveResponse, HttpGET.build(new URL(task.getUrl())));

            httpRetrieveResponse = httpClient.call();

            sender.tell(new SendResponse(httpRetrieveResponse), getSelf());
        }
    }
}
