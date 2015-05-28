package harvester.cluster;

import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingJobSubTask;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.*;

public class SlaveDownloaderTest {

    private static org.apache.logging.log4j.Logger LOG = LogManager.getLogger(SlaveDownloaderTest.class.getName());
    private static String PATH_PREFIX = Paths.get("harvester-server/src/test/resources/downloader").toAbsolutePath().toString() + "/";
    private static final String pathOnDisk = PATH_PREFIX + "current_image1.jpeg";
    private static final String text1GitHubUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();


    @After
    public void tearDown() throws Exception {
        Files.delete(Paths.get(pathOnDisk));
    }

//    @Test
//    public void canDownloadUnconditionally() throws Exception {
//        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
//        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
//        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
//        final RetrieveUrl task = new RetrieveUrl("id-1", text1GitHubUrl, httpRetrieveConfig, "jobid-1",
//                "referenceid-1", Collections.<String, String>emptyMap(),
//                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
//                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);
//
//        slaveDownloader.downloadAndStoreInHttpRetrievResponse(response, task);
//
//        assertEquals(ResponseState.COMPLETED, response.getState());
//
//        assertEquals(pathOnDisk, response.getAbsolutePath());
//        assertNotNull(response.getSourceIp());
//        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
//        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
//        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
//        assertEquals(response.getContentSizeInBytes().longValue(), 1399538);
//        assertEquals(Files.size(Paths.get(pathOnDisk)), 1399538l);
//
//    }


    @Test
    public void canDownloadConditionallyAndSkipDownloadWhenSameContentLengthResponseHeaderEntry() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
        final RetrieveUrl task = new RetrieveUrl("id-1", text1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>singletonMap("Content-Length", "1399538"),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrievResponse(response, task);

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
    public void canDownloadConditionallyAndSkipDownloadWhenDifferentContentLengthResponseHeaderEntry() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LOG);
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);
        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig();
        final RetrieveUrl task = new RetrieveUrl("id-1", text1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>singletonMap("Content-Length", "1399537"),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveDownloader.downloadAndStoreInHttpRetrievResponse(response, task);

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
