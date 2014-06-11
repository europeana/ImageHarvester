package eu.europeana.harvester.httpclient;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.httpclient.request.HttpGET;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseMemoryStorage;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class HttpClientTest {

    private HashedWheelTimer hashedWheelTimer = new HashedWheelTimer();
    private ChannelFactory channelFactory;
    private ExecutorService httpPool = Executors.newFixedThreadPool(1);

    private final List<String> downloadLinks = Arrays.asList(
            "https://androidnetworktester.googlecode.com/files/1mb.txt",
            "http://download.thinkbroadband.com/5MB.zip",
            "http://download.thinkbroadband.com/10MB.zip",
            "http://jwst.nasa.gov/images3/flightmirrorarrive1.jpg",
            "http://jwst.nasa.gov/images3/flightmirrorarrive2.jpg",
            "http://jwst.nasa.gov/images3/flightmirrorarrive3.jpg",
            "http://jwst.nasa.gov/images3/flightmirrorarrive4.jpg",
            "http://jwst.nasa.gov/images3/flightmirrorarrive5.jpg"
    );

    @Before
    public void setUp() throws Exception {
        ExecutorService bossPool = Executors.newCachedThreadPool();
        ExecutorService workerPool = Executors.newCachedThreadPool();

        channelFactory = new NioClientSocketChannelFactory(bossPool, workerPool);
    }

    @Test
    public void testDownloadWithBandwidthLimit() throws Exception {
        final long readWriteLimitKB = 500;

        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                readWriteLimitKB*1024l, readWriteLimitKB*1024l, Duration.ZERO, 0l, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);

        long startTime;
        for(String downloadLink : downloadLinks) {
            startTime = System.currentTimeMillis();

            HttpRetrieveResponse httpRetrieveResponse = new HttpRetrieveResponseMemoryStorage();
            HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer, httpRetrieveConfig, null,
                    httpRetrieveResponse, HttpGET.build(new URL(downloadLink)));
            Future<HttpRetrieveResponse> res = httpPool.submit(httpClient);

            HttpRetrieveResponse response = res.get();

            long runningTimeInMilliSec = System.currentTimeMillis() - startTime;
            long downloadSizeInBytes = response.getContentSizeInBytes();
            double downloadSpeedKB = (downloadSizeInBytes*1000) / (runningTimeInMilliSec*1024);

            boolean smallerSpeed = downloadSpeedKB < readWriteLimitKB;
            System.out.println("Speed: " + downloadSpeedKB);

            assertTrue(smallerSpeed);
        }
    }

    @Test
    public void testDownloadWithReadTimeLimit() throws Exception {
        final long readWriteLimitKB = 500;
        final long timeLimitInMilliSec = 500;
        final long toleranceInMilliSec = 1000;

        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                readWriteLimitKB*1024l, readWriteLimitKB*1024l, Duration.millis(timeLimitInMilliSec), 0l, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);

        long startTime;
        for(String downloadLink : downloadLinks) {
            startTime = System.currentTimeMillis();

            HttpRetrieveResponse httpRetrieveResponse = new HttpRetrieveResponseMemoryStorage();
            HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer, httpRetrieveConfig, null,
                    httpRetrieveResponse, HttpGET.build(new URL(downloadLink)));
            Future<HttpRetrieveResponse> res = httpPool.submit(httpClient);

            res.get();

            long runningTimeInMilliSec = System.currentTimeMillis() - startTime;

            boolean smallerSpeed = runningTimeInMilliSec < timeLimitInMilliSec + toleranceInMilliSec;

            assertTrue(smallerSpeed);
        }
    }

    @Test
    public void testDownloadWithSizeLimit() throws Exception {
        final long readWriteLimitKB = 500;
        final long sizeLimitKB = 500;
        final long toleranceInKB = 1000;

        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(Duration.millis(100),
                readWriteLimitKB*1024l, readWriteLimitKB*1024l, Duration.ZERO, sizeLimitKB, true,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);

        for(String downloadLink : downloadLinks) {
            HttpRetrieveResponse httpRetrieveResponse = new HttpRetrieveResponseMemoryStorage();
            HttpClient httpClient = new HttpClient(channelFactory, hashedWheelTimer, httpRetrieveConfig, null,
                    httpRetrieveResponse, HttpGET.build(new URL(downloadLink)));
            Future<HttpRetrieveResponse> res = httpPool.submit(httpClient);

            HttpRetrieveResponse response = res.get();

            long downloadSizeInBytes = response.getContentSizeInBytes();

            boolean smallerSpeed = downloadSizeInBytes < sizeLimitKB + toleranceInKB;

            assertTrue(smallerSpeed);
        }
    }

}
