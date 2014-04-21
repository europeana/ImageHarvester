package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseMemoryStorage;
import eu.europeana.harvester.httpclient.response.HttpRetriveResponse;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class Main {


    public static void main(String[] args) throws Exception {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
        ExecutorService httpPool = Executors.newCachedThreadPool();
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


        long startTime = System.currentTimeMillis();

        HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100), 1000*1024l /* write */, 10*1024l /* read */, Duration.ZERO, 100*1024l, true);

        List<Future<HttpRetriveResponse>> responses = new ArrayList<Future<HttpRetriveResponse>>();
        for (final String downloadLink : downloadLinks) {
            HttpRetriveResponse httpRetriveResponse = new HttpRetrieveResponseMemoryStorage();
            HttpClient httpClient = new HttpClient(channelFactory,hashedWheelTimer,httpRetrieveConfig,httpRetriveResponse, HttpGET.build(new URL(downloadLink)));
            Future<HttpRetriveResponse> res = httpPool.submit(httpClient);
            responses.add(res);
        }


        while(true) {
            for (Future<HttpRetriveResponse> res : responses) {
                if (res.get().getContent() != null) {
                    System.out.println(res.get().getUrl()+res.get().getState().toString()+" -"+res.get().getContent().length);
                } else {
                    System.out.println(res.get().getUrl()+"Null content");
                }
            }
            Thread.sleep(1000);
        }

//        long endTime = System.currentTimeMillis();
//        long totalTime = endTime - startTime;
//        System.out.println("Running time: " + totalTime / 1000);

    }

}

