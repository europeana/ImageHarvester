package eu.europeana.harvester.cluster.slave.downloading;

import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ProcessingJobLimits;
import eu.europeana.harvester.domain.ProcessingJobSubTask;
import eu.europeana.harvester.domain.ProcessingJobTaskDocumentReference;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.logging.log4j.LogManager;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import eu.europeana.harvester.TestUtils;
public class SlaveLinkCheckerTest {

    private static org.apache.logging.log4j.Logger LOG = LogManager.getLogger(SlaveDownloaderTest.class.getName());
    private static final String text1GitHubUrl = TestUtils.GitHubUrl_PREFIX + TestUtils.Image1;

    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    @Test
    public void canLinkCheckWithDefaultLimits() throws Exception {
        final SlaveLinkChecker slaveLinkChecker = new SlaveLinkChecker();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE,null);
        final RetrieveUrl task = new RetrieveUrl(text1GitHubUrl, new ProcessingJobLimits(), DocumentReferenceTaskType.CHECK_LINK,
                "referenceid-1","a", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveLinkChecker.downloadAndStoreInHttpRetrievResponse(response, task);

        assertEquals(RetrievingState.COMPLETED, response.getState());

        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
    }

    @Test
    public void canLinkCheckWithDefaultLimits1() throws Exception {
        final SlaveLinkChecker slaveLinkChecker = new SlaveLinkChecker();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE,null);
        final ProcessingJobLimits limits = new ProcessingJobLimits(
                100*1000l /* retrievalTerminationThresholdTimeLimitInMillis  */,
                5 * 1000l /* retrievalTerminationThresholdReadPerSecondInBytes */,
                10l /* retrievalConnectionTimeoutInMillis - IT SHOULD FAIL BECAUSE OF THIS  */,
                10 /* retrievalMaxNrOfRedirects */,
                100 * 1000l /* processingTerminationThresholdTimeLimitInMillis */);

        final RetrieveUrl task = new RetrieveUrl(text1GitHubUrl, limits,DocumentReferenceTaskType.CHECK_LINK, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveLinkChecker.downloadAndStoreInHttpRetrievResponse(response, task);

        assertEquals(RetrievingState.FINISHED_TIME_LIMIT, response.getState());

        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertEquals(response.getRetrievalDurationInMilliSecs().longValue(), 0);
    }

    @Test
    public void canFailLinkCheckWhenUrlIsNonExistentWithDefaultLimits() throws Exception {
        final SlaveLinkChecker slaveLinkChecker = new SlaveLinkChecker();
        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.NO_STORAGE,null);
        final RetrieveUrl task = new RetrieveUrl(text1GitHubUrl+"-some-extra-nonsense", new ProcessingJobLimits(), DocumentReferenceTaskType.CHECK_LINK, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                        "source-reference-1", Collections.<ProcessingJobSubTask>emptyList()), null);

        slaveLinkChecker.downloadAndStoreInHttpRetrievResponse(response, task);

        assertEquals(RetrievingState.ERROR, response.getState());
        assertEquals(404, response.getHttpResponseCode().longValue());

        assertNotNull(response.getSourceIp());
        assertTrue(response.getSocketConnectToDownloadStartDurationInMilliSecs() > 5);
        assertTrue(response.getCheckingDurationInMilliSecs() > 50);
        assertTrue(response.getRetrievalDurationInMilliSecs() > 50);
    }


}
