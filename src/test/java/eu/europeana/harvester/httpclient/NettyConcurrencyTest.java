package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.response.*;
import junit.framework.TestCase;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;
import org.junit.Test;
import test.LinkParser;

import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class NettyConcurrencyTest extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();

        //ExecutorService httpPool = Executors.
        ExecutorService bossPool = Executors.newFixedThreadPool(1);
        ExecutorService workerPool = Executors.newFixedThreadPool(16);

        ChannelFactory channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);

        final int nrOfLinks = 500;
        final String outputFileName = "outLinkCheck";

//        LinkParser linkParser = new LinkParser(nrOfLinks, outputFileName);
//        linkParser.start();

        //long start = System.currentTimeMillis();

        final File links = new File("./src/main/resources/TestLinks/" + outputFileName);

        HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                25*1024l /* write */, 25*1024l /* read */, Duration.ZERO, 0*1024l, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 10000);

        List<Future<HttpRetrieveResponse>> responses = new ArrayList<Future<HttpRetrieveResponse>>();
        List<HttpClient> httpClients = new ArrayList<HttpClient>();

        try {
            BufferedReader br = new BufferedReader(new FileReader(links));

            String line = ""; int i = 0;

            while((line = br.readLine()) != null) {
                System.out.println(line);
                HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
                HttpRetrieveResponse httpRetrieveResponse =
                        httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, "/home/norbert/Temp/" + i++);
                HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer, httpRetrieveConfig, null,
                        httpRetrieveResponse, HttpGET.build(new URL(line)));

                httpClients.add(httpClient);
            }

            long start = System.currentTimeMillis();
            long sum = 0;
            List<String> doneLinks = new ArrayList<String>();
            System.out.println("___________");
            for(HttpClient httpClient : httpClients) {
                HttpRetrieveResponse response = httpClient.call();
                System.out.println(response.getUrl() + " " + response.getState().toString());
                if(response.getHttpResponseCode() >= 200 && response.getHttpResponseCode() < 300 && response.getRetrievalDurationInMilliSecs() != null)
                    sum += response.getRetrievalDurationInMilliSecs()/1000.0;
                System.out.println(Thread.activeCount());
            }

            System.out.println(sum);
            System.out.println((System.currentTimeMillis() - start) / 1000.0);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
