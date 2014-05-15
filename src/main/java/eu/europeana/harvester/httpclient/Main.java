package eu.europeana.harvester.httpclient;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import eu.europeana.harvester.cluster.master.ClusterMasterActor;
import eu.europeana.harvester.cluster.slave.SlaveActor;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class Main {

    public static void main(String[] args) throws Exception {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        ExecutorService httpPool = Executors.newFixedThreadPool(1);
        ExecutorService bossPool = Executors.newCachedThreadPool();
        ExecutorService workerPool = Executors.newCachedThreadPool();

        ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        List<String> downloadLinks = Arrays.asList(
                "http://edition.cnn.com",
                "http://download.thinkbroadband.com/5MB.zip",
                "http://download.thinkbroadband.com/10MB.zip",
                "http://download.thinkbroadband.com/20MB.zip",
                "http://download.thinkbroadband.com/50MB.zip",
                "http://download.thinkbroadband.com/100MB.zip",
                "http://jwst.nasa.gov/images3/flightmirrorarrive1.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive2.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive3.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive4.jpg",
                "http://jwst.nasa.gov/images3/flightmirrorarrive5.jpg"
        );

//        long startTime = System.currentTimeMillis();

        final ActorSystem system = ActorSystem.create("imageHarvester");

        final ActorRef slave = system.actorOf(Props.create(SlaveActor.class, channelFactory, hashedWheelTimer),
                "slave");

        final ActorRef master = system.actorOf(Props.create(ClusterMasterActor.class, downloadLinks, slave),
                "master");

        master.tell("start", ActorRef.noSender());


//        HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
//                1000*1024l /* write */, 500*1024l /* read */, Duration.ZERO, 0*1024l, true);
//
//        List<Future<HttpRetrieveResponse>> responses = new ArrayList<Future<HttpRetrieveResponse>>();
//        for (final String downloadLink : downloadLinks) {
//            HttpRetrieveResponse httpRetriveResponse = new HttpRetrieveResponseMemoryStorage();
//            HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer, httpRetrieveConfig,
//                    httpRetriveResponse, HttpGET.build(new URL(downloadLink)));
//            Future<HttpRetrieveResponse> res = httpPool.submit(httpClient);
//            responses.add(res);
//        }
//
//        System.out.println("Start...");
//
//        int nrTasks = downloadLinks.size(), doneTasks = 0;
//        List<String> doneLinks = new ArrayList<String>();
//
//        while(doneTasks != nrTasks) {
//            for (Future<HttpRetrieveResponse> res : responses) {
//                if(res.isDone()) {
//                    HttpRetrieveResponse response = res.get();
//                    System.out.println(response.getUrl() + " " + response.getState().toString() + " + " +
//                            response.getContent().length);
//                    if(!doneLinks.contains(String.valueOf(response.getUrl()))) {
//                        doneLinks.add(String.valueOf(response.getUrl()));
//                        doneTasks++;
//                    }
//                }
//            }
//
//            System.out.println(Thread.activeCount());
//            Thread.sleep(1000);
//        }
//
//
//        System.out.println("Done");
//        System.exit(0);

//        long endTime = System.currentTimeMillis();
//        long totalTime = endTime - startTime;
//        System.out.println("Running time: " + totalTime / 1000);

    }

}
