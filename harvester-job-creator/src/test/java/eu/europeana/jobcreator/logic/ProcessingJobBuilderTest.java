package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by salexandru on 02.06.2015.
 */
public class ProcessingJobBuilderTest {
    private final static ReferenceOwner owner = new ReferenceOwner("", "", "");
    private final static ProcessingJobCreationOptions falseOption = new ProcessingJobCreationOptions(false);
    private final static ProcessingJobCreationOptions trueOption = new ProcessingJobCreationOptions(true);


    @Test(expected = IllegalArgumentException.class)
    public void test_EdmObj_NullOption() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmObjectUrlJobs("", owner, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EdmHasView_NullOption() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmHasViewUrlsJobs(Arrays.asList(""), owner, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EdmIsShownBy_NullOption() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmIsShownByUrlJobs("", owner, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EdmIsShownAt_NullOption() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmIsShownAtUrlJobs("", owner, null);
    }

    @Test(expected = MalformedURLException.class)
    public void test_EdmObj_InvalidUrl() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmObjectUrlJobs(UUID.randomUUID().toString(), owner, falseOption);
    }

    @Test(expected = MalformedURLException.class)
    public void test_EdmHasView_InvalidUrl() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmHasViewUrlsJobs(Arrays.asList(UUID.randomUUID().toString()), owner, falseOption);
    }

    @Test(expected = MalformedURLException.class)
    public void test_EdmIsShownBy_InvalidUrl() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmIsShownByUrlJobs(UUID.randomUUID().toString(), owner, falseOption);
    }

    @Test(expected = MalformedURLException.class)
    public void test_EdmIsShownAt_InvalidUrl() throws MalformedURLException, UnknownHostException {
        ProcessingJobBuilder.edmIsShownAtUrlJobs(UUID.randomUUID().toString(), owner, falseOption);
    }

    @Test
    public void test_EdmObj_ValidUrl_DefaultOption() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com", owner,
                                                                                    falseOption);

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 0, 1, 2,
                      ProcessingJobSubTaskType.COLOR_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL);

    }

    @Test
    public void test_EdmObj_ValidUrl_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com", owner,
                                                                                    trueOption);

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 0, 1, 2,
                      ProcessingJobSubTaskType.COLOR_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL);
    }

    @Test
    public void testEdmHasViewUrls_OneElement_DefaultOption() throws MalformedURLException, UnknownHostException {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, falseOption);
        assertEquals(urls.size(), jobs.size());

        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2, ProcessingJobSubTaskType.values());
    }



    @Test
    public void testEdmHasViewUrls_TwoElements_DefaultOption()throws MalformedURLException, UnknownHostException  {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");
        urls.add("http://www.facebook.com");


        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, falseOption);

        assertEquals(urls.size(), jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2,
                      ProcessingJobSubTaskType.values());

    }

    @Test
    public void testEdmHasViewUrls_ManyElements()throws MalformedURLException, UnknownHostException  {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");
        urls.add("http://www.facebook.com");
        urls.add("http://www.yahoo.com");
        urls.add("http://www.wikipedia.org");
        urls.add("http://de.wikipedia.org/wiki");
        urls.add("http://www.w3schools.com/");
        urls.add("http://www.skype.com");

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, falseOption);

        assertEquals(urls.size(), jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2, ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmHasViewUrls_OneElement_ForceUnconditionalOption() throws MalformedURLException, UnknownHostException {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, trueOption);
        assertEquals(urls.size(), jobs.size());

        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,  1, 1, 2, ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmHasViewUrls_ManyElements_ForceUnconditionalDownload()throws MalformedURLException, UnknownHostException  {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");
        urls.add("http://www.facebook.com");
        urls.add("http://www.yahoo.com");
        urls.add("http://www.wikipedia.org");
        urls.add("http://de.wikipedia.org/wiki");
        urls.add("http://www.w3schools.com/");
        urls.add("http://www.skype.com");

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, trueOption);

        assertEquals(urls.size(), jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 1, 1, 2, ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmIsShownBy_ValidUrl_DefaultOption() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownByUrlJobs("http://www.google.com", owner,
                                                                                       falseOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2,
                      ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmIsShownBy_ValidUrl_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownByUrlJobs("http://www.google.com", owner,
                                                                                       trueOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 1, 1, 2,
                      ProcessingJobSubTaskType.values());
    }


    @Test
    public void testEdmIsShownAt_ValidUrl_DefaultOption() throws MalformedURLException, UnknownHostException  {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownAtUrlJobs("http://www.google.com", owner,
                                                                                       falseOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CHECK_LINK, 0, 0, 0);
    }

    @Test
    public void testEdmIsShownAt_ValidUrl_ForceUnconditionalDownload() throws MalformedURLException, UnknownHostException  {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownAtUrlJobs("http://www.google.com", owner, trueOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CHECK_LINK, 0, 0, 0);
    }

    private void validateTasks (List<ProcessingJobTuple> jobs, DocumentReferenceTaskType taskType,
                                int metaDataExtraction, int colorExtraction, int generateThumbnail,
                                ProcessingJobSubTaskType ... tasks) {
        for (final ProcessingJobTuple job: jobs) {
            assertEquals(1, job.getProcessingJob().getTasks().size());
            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(taskType, job.getProcessingJob().getTasks().get(0).getTaskType());

            final ProcessingJobTaskDocumentReference jobReference = job.getProcessingJob().getTasks().get(0);

            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(taskType, jobReference.getTaskType());
            assertEquals(metaDataExtraction + colorExtraction + generateThumbnail, jobReference.getProcessingTasks().size());
            int countThumbnailTasks = 0, countColorExtractions = 0, countMetaExtractions = 0;

            for (final ProcessingJobSubTask subTask : jobReference.getProcessingTasks()) {
                if (ArrayUtils.contains(tasks, subTask.getTaskType())) {
                    switch (subTask.getTaskType()) {
                        case COLOR_EXTRACTION: ++countColorExtractions; break;
                        case GENERATE_THUMBNAIL: ++countThumbnailTasks; break;
                        case META_EXTRACTION: ++countMetaExtractions; break;
                        default: fail ("Unknown subtask type " + subTask.getTaskType());
                    }
                }
                else {
                    fail("Generated wrong subtask type: " + subTask.getTaskType().name());
                }
            }

            assertEquals (colorExtraction, countColorExtractions);
            assertEquals (generateThumbnail, countThumbnailTasks);
            assertEquals (metaDataExtraction, countMetaExtractions);
        }
    }

}
