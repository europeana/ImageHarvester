package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.messages.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import org.joda.time.Duration;

import java.util.List;

public class MasterActor extends UntypedActor {

    private List<String> urls;

    private ActorRef slaveActor;

    private HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
            1000*1024l /* write */, 1000*1024l /* read */, Duration.ZERO, 0l, true);

    public MasterActor(List<String> urls, ActorRef slaveActor) {
        this.urls = urls;
        this.slaveActor = slaveActor;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof List) {
            urls = (List<String>) message;
        } else
        if(message instanceof String && message.equals("start")) {
            for(String url : urls) {
                slaveActor.tell(new RetrieveUrl(url, httpRetrieveConfig), getSelf());
            }
        } else
        if(message instanceof StartedUrl) {
            System.out.println("From " + getSender() + " to master: " + ((StartedUrl)message).getUrl() + " started.");
        } else
        if(message instanceof SendResponse) {
            System.out.println("From " + getSender() + " to master: " +
                    ((SendResponse)message).getHttpRetrieveResponse().getUrl() + " " +
                    ((SendResponse)message).getHttpRetrieveResponse().getContentSizeInBytes() + " done.");
        }
    }
}
