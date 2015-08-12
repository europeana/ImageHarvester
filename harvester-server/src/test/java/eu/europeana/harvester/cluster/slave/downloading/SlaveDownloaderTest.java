package eu.europeana.harvester.cluster.slave.downloading;

import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static eu.europeana.harvester.TestUtils.*;
import static org.junit.Assert.*;

public class SlaveDownloaderTest {

    private static org.apache.logging.log4j.Logger LOG = LogManager.getLogger(SlaveDownloaderTest.class.getName());
    private static final String pathOnDisk = PATH_DOWNLOADED + "original_image1.jpeg";
    private static final String image1GitHubUrl = GitHubUrl_PREFIX + Image1;

    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    @Before
    public void setUp() throws IOException {
        Files.createDirectories(Paths.get(PATH_DOWNLOADED));
    }

    @After
    public void tearDown() throws Exception {
        if (new File(pathOnDisk).exists()) new File(pathOnDisk).delete();
    }

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Test
    public void canAbortUnconditionalDownloadWhenSocketConnectionTimeExceeded() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);

        final ProcessingJobLimits limits = new ProcessingJobLimits(
                100 * 1000l /* retrievalTerminationThresholdTimeLimitInMillis */,
                5 * 1000l /* retrievalTerminationThresholdReadPerSecondInBytes */,
                1l /* retrievalConnectionTimeoutInMillis - IT SHOULD FAIL BECAUSE OF THIS */,
                10 /* retrievalMaxNrOfRedirects */,
                100 * 1000l /* processingTerminationThresholdTimeLimitInMillis */);

        final RetrieveUrl task = new RetrieveUrl(
                image1GitHubUrl,
                limits,
                DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                "jobid-1",
                "referenceid-1",
                Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1",
                        Collections.<ProcessingJobSubTask>emptyList()
                ),
                null,new ReferenceOwner("unknown","unknwon","unknown")
        );

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.FINISHED_TIME_LIMIT, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertEquals(response.getContentSizeInBytes().longValue(), 0);
    }


    @Test
    public void canAbortUnconditionalDownloadWhenTerminationThresholdTimeLimitExceeded() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);

        final ProcessingJobLimits limits = new ProcessingJobLimits(
                10l /* retrievalTerminationThresholdTimeLimitInMillis - IT SHOULD FAIL BECAUSE OF THIS */,
                5 * 1000l /* retrievalTerminationThresholdReadPerSecondInBytes */,
                10 * 1000l /* retrievalConnectionTimeoutInMillis  */,
                10 /* retrievalMaxNrOfRedirects */,
                100 * 1000l /* processingTerminationThresholdTimeLimitInMillis */);

        final RetrieveUrl task = new RetrieveUrl(image1GitHubUrl, limits, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.FINISHED_TIME_LIMIT, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertEquals(response.getContentSizeInBytes().longValue(), 0);
    }


    @Test
    public void canDownloadUnconditionallyWithDefaultLimits() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);

        final ProcessingJobLimits limits = new ProcessingJobLimits();

        final RetrieveUrl task = new RetrieveUrl(image1GitHubUrl, limits, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.COMPLETED, response.getState());

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
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);

        final ProcessingJobLimits limits = new ProcessingJobLimits();

        final RetrieveUrl task = new RetrieveUrl(image1GitHubUrl + "-some-stupid-extra", limits,DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.ERROR, response.getState());
        assertEquals(404, response.getHttpResponseCode().intValue());

        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
        assertEquals(response.getContentSizeInBytes().longValue(), 0);

    }

    @Test
    public void cannotDownloadNullUrlUnconditionallyWithDefaultLimits() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);

        final ProcessingJobLimits limits = new ProcessingJobLimits();

        final RetrieveUrl task = new RetrieveUrl(null /* IT SHOULD FAIL BECAUSE OF THIS */, limits,DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.ERROR, response.getState());

    }


    @Test
    public void canDownloadConditionallyAndSkipDownloadWhenSameContentLengthResponseHeaderEntry() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final ProcessingJobLimits limits = new ProcessingJobLimits();

        final RetrieveUrl task = new RetrieveUrl(image1GitHubUrl,
                                                 limits,
                                                 DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                                                 "jobid-1",
                                                 "referenceid-1",
                                                 Collections.singletonMap("Content-Length".toLowerCase(), "1399538"),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.COMPLETED, response.getState());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() < 5000);
        assertTrue(response.getContentSizeInBytes().longValue() == 0);
        assertFalse(new File(pathOnDisk).exists());
    }


    @Test
    public void canDownloadConditionallyAndDownloadWhenDifferentContentLengthResponseHeaderEntry() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final ProcessingJobLimits limits = new ProcessingJobLimits();

        final RetrieveUrl task = new RetrieveUrl(image1GitHubUrl, limits,DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, "jobid-1",
                "referenceid-1", Collections.singletonMap("Content-Length".toLowerCase(), "1399537"),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.COMPLETED, response.getState());
        assertFalse(response.getResponseHeaders().isEmpty());

        assertEquals(pathOnDisk, response.getAbsolutePath());
        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
        assertEquals(response.getContentSizeInBytes().longValue(), 1399538);
        assertEquals(Files.size(Paths.get(pathOnDisk)), 1399538);

    }

}
