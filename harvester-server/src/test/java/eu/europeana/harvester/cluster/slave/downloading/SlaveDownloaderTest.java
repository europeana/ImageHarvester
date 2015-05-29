package eu.europeana.harvester.cluster.slave.downloading;

import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingJobSubTask;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.junit.Before;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.*;

public class SlaveDownloaderTest {

    private static org.apache.logging.log4j.Logger LOG = LogManager.getLogger(SlaveDownloaderTest.class.getName());
    private static String PATH_PREFIX = Paths.get("harvester-server/src/test/resources/downloader").toAbsolutePath().toString() + "/";
    private static final String pathOnDisk = PATH_PREFIX + "original_image1.jpeg";
    private static final String image1GitHubUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    @Before
    public void setUp() throws IOException {
        Files.createDirectories(Paths.get(PATH_PREFIX));
    }

    @After
    public void tearDown() throws Exception {
        Files.delete(Paths.get(pathOnDisk));
    }

    @Test
    public void canAbortUnconditionalDownloadWhenSocketConnectionTimeExceeded() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(
        Duration.millis(0),
        0l,
        0l,
        5*1000l, /* terminationThresholdReadPerSecondInBytes */
                Duration.standardSeconds(100) /* terminationThresholdTimeLimit */,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, /* taskType */
        1 /* connectionTimeoutInMillis - IT SHOULD FAIL BECAUSE OF THIS */,
                10 /* maxNrOfRedirects */
        );
        final RetrieveUrl task = new RetrieveUrl("id-1",
                                                 image1GitHubUrl,
                                                 httpRetrieveConfig,
                                                 "jobid-1",
                                                 "referenceid-1",
                                                 Collections.<String, String>emptyMap(),
                                                 new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                                                                         "source-reference-1",
                                                                                         Collections.<ProcessingJobSubTask>emptyList()
                                                                                        ),
                                                 null
                                                );

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(ResponseState.FINISHED_TIME_LIMIT, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertEquals(response.getContentSizeInBytes().longValue(), 0);
    }


    @Test
    public void canAbortUnconditionalDownloadWhenTerminationThresholdTimeLimitExceeded() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(
                Duration.millis(0),
                0l,
                0l,
                5*1000l, /* terminationThresholdReadPerSecondInBytes */
                Duration.millis(10) /* terminationThresholdTimeLimit - IT SHOULD FAIL BECAUSE OF THIS */,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, /* taskType */
                (int)Duration.standardSeconds(10).getMillis() /* connectionTimeoutInMillis */,
                10 /* maxNrOfRedirects */
        );
        final RetrieveUrl task = new RetrieveUrl("id-1", image1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(ResponseState.FINISHED_TIME_LIMIT, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertEquals(response.getContentSizeInBytes().longValue(), 0);
    }



    @Test
    public void canDownloadUnconditionallyWithDefaultLimits() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
        final RetrieveUrl task = new RetrieveUrl("id-1", image1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(ResponseState.COMPLETED, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
        assertEquals(response.getContentSizeInBytes().longValue(), 1399538);
        assertEquals(Files.size(Paths.get(pathOnDisk)), 1399538l);

    }

    @Test
    public void canDownloadNonExistentUrlUnconditionallyWithDefaultLimits() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
        final RetrieveUrl task = new RetrieveUrl("id-1", image1GitHubUrl +"-some-stupid-extra", httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(ResponseState.ERROR, response.getState());
        assertEquals(404, response.getHttpResponseCode().intValue());

        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
        assertEquals(response.getContentSizeInBytes().longValue(), 0);

    }


    @Test
    public void canDownloadConditionallyAndSkipDownloadWhenSameContentLengthResponseHeaderEntry() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
        final RetrieveUrl task = new RetrieveUrl("id-1", image1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>singletonMap("Content-Length", "1399538"),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(ResponseState.COMPLETED, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
        assertEquals(response.getContentSizeInBytes().longValue(), 0);
        assertEquals(Files.size(Paths.get(pathOnDisk)), 0);

    }


    @Test
    public void canDownloadConditionallyAndDownloadWhenDifferentContentLengthResponseHeaderEntry() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
        final RetrieveUrl task = new RetrieveUrl("id-1", image1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>singletonMap("Content-Length", "1399537"),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(ResponseState.COMPLETED, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
        assertEquals(response.getContentSizeInBytes().longValue(), 1399538);
        assertEquals(Files.size(Paths.get(pathOnDisk)), 1399538);

    }

}
