package eu.europeana.jobcreator;

import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.logic.ProcessingJobBuilder;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class JobCreatorTest {
    private final static String collectionId = "2023831_AG-EU_LinkedHeritage_Rybinsk";
    private final static String providerId = "2023831";
    private final static String recordId = "/2023831/kng_item_item_jsf_id_105436";
    private final static String executionId = "myUIMPlugin1";

    private final static String url = "http://www.google.com";
    private final static List<String> urls = Arrays.asList("http://www.google.com", "http://www.yahoo.com", "http://www.facebook.com");

    private final static ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId, executionId);

    private final static ProcessingJobCreationOptions falseOption = new ProcessingJobCreationOptions(false);
    private final static ProcessingJobCreationOptions trueOption = new ProcessingJobCreationOptions(true);

    @Test (expected = IllegalArgumentException.class)
    public void test_AllArgumentNull() throws MalformedURLException, UnknownHostException {
        JobCreator.createJobs(null, null, null, null, null, null, null, null,JobPriority.NORMAL.getPriority());
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_NullOption() throws MalformedURLException, UnknownHostException {
        JobCreator.createJobs(collectionId, providerId, recordId, executionId, "", null, "", "", null);
    }

    @Test
    public void test_edmObjectTasks_WithoutOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmObjectUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    url, null, null, null,JobPriority.NORMAL.getPriority()
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmObjectTasks_DefaultOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmObjectUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    url, null, null, null,JobPriority.NORMAL.getPriority(), falseOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmObjectTasks_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmObjectUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), trueOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    url, null, null, null,JobPriority.NORMAL.getPriority(), trueOption
                                                                   );

        checkEquals(expectedJobs, jobs);
    }

    @Test
    public void test_edmHasViewUrls_WithoutOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, urls, null, null,JobPriority.NORMAL.getPriority()
                                                                   );

        checkEquals(expectedJobs, jobs);
    }

    @Test
    public void test_edmHasViewUrls_DefaultOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, urls, null, null,JobPriority.NORMAL.getPriority(), falseOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmHasViewUrls_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner,JobPriority.NORMAL.getPriority(), trueOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, urls, null, null,JobPriority.NORMAL.getPriority(), trueOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmIsShownByUrl_WithoutOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmIsShownByUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, url, null,JobPriority.NORMAL.getPriority()
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmIsShownByUrl_DefaultOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmIsShownByUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, url, null,JobPriority.NORMAL.getPriority(), falseOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmIsShownByUrl_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmIsShownByUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), trueOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, url, null,JobPriority.NORMAL.getPriority(), trueOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }




    @Test
    public void test_edmIsShownAtUrl_WithoutOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmIsShownAtUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, null, url,JobPriority.NORMAL.getPriority()
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmIsShownAtUrl_DefaultOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmIsShownAtUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, null, url,JobPriority.NORMAL.getPriority(), falseOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_edmIsShownAtUrl_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = ProcessingJobBuilder.edmIsShownAtUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), trueOption);
        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, null, url,JobPriority.NORMAL.getPriority(), trueOption
                                                                   );

        checkEquals (expectedJobs, jobs);
    }

    @Test
    public void test_AllTasks() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = new ArrayList<>();
        expectedJobs.addAll(ProcessingJobBuilder.edmObjectUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmIsShownAtUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));

        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    url, urls, url, url,JobPriority.NORMAL.getPriority()
                                                                   );
        checkEquals(expectedJobs, jobs);
    }

    @Test
    public void test_EdmObject_EdmHasViews() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = new ArrayList<>();
        expectedJobs.addAll(ProcessingJobBuilder.edmObjectUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner,JobPriority.NORMAL.getPriority(), falseOption));

        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    url, urls, null, null,JobPriority.NORMAL.getPriority()
                                                                   );
        checkEquals(expectedJobs, jobs);
    }

    @Test
    public void test_EdmIsShownBy_EdmIsShownAt() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = new ArrayList<>();
        expectedJobs.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmIsShownAtUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));

        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    null, null, url, url,JobPriority.NORMAL.getPriority()
                                                                   );
        checkEquals(expectedJobs, jobs);
    }

    @Test
    public void test_EdmObject_EdmHasViews_EdmIsShownBy() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> expectedJobs = new ArrayList<>();
        expectedJobs.addAll(ProcessingJobBuilder.edmObjectUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner,JobPriority.NORMAL.getPriority(), falseOption));
        expectedJobs.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(url, owner,JobPriority.NORMAL.getPriority(), falseOption));

        final List<ProcessingJobTuple> jobs = JobCreator.createJobs(collectionId, providerId, recordId, executionId,
                                                                    url, urls, url, null,JobPriority.NORMAL.getPriority()
                                                                   );
        checkEquals(expectedJobs, jobs);
    }

    public void checkEquals (final List<ProcessingJobTuple> expectedJobs, List<ProcessingJobTuple> generatedJobs) {
        if (null == expectedJobs) {
            assertNull (generatedJobs);
            return;
        }
        assertNotNull(generatedJobs);
        assertEquals (expectedJobs.size(), generatedJobs.size());

        final Iterator<ProcessingJobTuple> expectedJobsIter = expectedJobs.iterator();
        final Iterator<ProcessingJobTuple> generatedJobsIter = generatedJobs.iterator();

        while (expectedJobsIter.hasNext() && generatedJobsIter.hasNext()) {
            final ProcessingJobTuple expectedJob = expectedJobsIter.next();
            final ProcessingJobTuple generatedJob = generatedJobsIter.next();

            assertEquals(expectedJob.getProcessingJob().getPriority(), generatedJob.getProcessingJob().getPriority());
            assertTrue(EqualsBuilder.reflectionEquals(owner, generatedJob.getProcessingJob().getReferenceOwner()));
            assertEquals(expectedJob.getProcessingJob().getState(), generatedJob.getProcessingJob().getState());

            assertEquals (1, expectedJob.getProcessingJob().getTasks().size());
            assertEquals (1, generatedJob.getProcessingJob().getTasks().size());

            final ProcessingJobTaskDocumentReference expectedTasks = expectedJob.getProcessingJob().getTasks().get(0);
            final ProcessingJobTaskDocumentReference generatedTasks = generatedJob.getProcessingJob().getTasks().get(0);


            assertEquals (expectedTasks.getSourceDocumentReferenceID(), generatedTasks.getSourceDocumentReferenceID());
            assertEquals (expectedTasks.getTaskType(), generatedTasks.getTaskType());

            checkSubTasksEquals (expectedTasks.getProcessingTasks(), generatedTasks.getProcessingTasks());


            assertTrue(EqualsBuilder.reflectionEquals(expectedJob.getSourceDocumentReference(),
                                                      generatedJob.getSourceDocumentReference()));
        }
    }

    private void checkSubTasksEquals (List<ProcessingJobSubTask> expectedProcessingTasks,
                                      List<ProcessingJobSubTask> generatedProcessingTasks) {
        if (null == expectedProcessingTasks) {
            assertNull (generatedProcessingTasks);
            return;
        }
        assertNotNull (generatedProcessingTasks);
        assertEquals(expectedProcessingTasks.size(), generatedProcessingTasks.size());
        assertArrayEquals(expectedProcessingTasks.toArray(), generatedProcessingTasks.toArray());
    }
}
