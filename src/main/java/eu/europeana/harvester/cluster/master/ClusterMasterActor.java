package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.FromConfig;
import eu.europeana.harvester.cluster.messages.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import org.joda.time.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class ClusterMasterActor extends UntypedActor {

    private List<String> urls;

    ActorRef slaveActor = getContext().actorOf(FromConfig.getInstance().props(), "nodeMasterRouter");

    private HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
            1000*1024l /* write */, 1000*1024l /* read */, Duration.ZERO, 0*1024l, true);

    public ClusterMasterActor(List<String> urls) {
        this.urls = urls;
    }

    @Override
    public void preStart() throws Exception {
        System.out.println("Started");
        getContext().setReceiveTimeout(scala.concurrent.duration.Duration.create(10, TimeUnit.SECONDS));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof List) {
            urls = (List<String>) message;
        } else
        if(message instanceof String && message.equals("start")) {
            for(String url : urls) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                slaveActor.tell(new RetrieveUrl(url, httpRetrieveConfig), getSelf());
            }
        } else
        if(message instanceof StartedUrl) {
            System.out.println("From " + getSender() + " to master: " +
                    ((StartedUrl)message).getUrl() + " started.");
        } else
        if(message instanceof SendResponse) {
            System.out.println("From " + getSender() + " to master: " +
                    ((SendResponse)message).getHttpRetrieveResponse().getUrl() + " " +
                    ((SendResponse)message).getHttpRetrieveResponse().getContentSizeInBytes() + " done.");
        } else
        if(message instanceof DoneDownload) {
            System.out.println("From " + getSender() + " to master: " + ((DoneDownload)message).getUrl() + " done.");
        }
    }
}
