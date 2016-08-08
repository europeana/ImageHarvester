package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

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
    public void test_EdmObj_NullOption() throws ExecutionException {
        ProcessingJobBuilder.edmObjectUrlJobs("", owner, JobPriority.NORMAL.getPriority(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EdmHasView_NullOption() throws ExecutionException {
        ProcessingJobBuilder.edmHasViewUrlsJobs(Arrays.asList(""), owner, JobPriority.NORMAL.getPriority(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EdmIsShownBy_NullOption() throws ExecutionException {
        ProcessingJobBuilder.edmIsShownByUrlJobs("", owner, JobPriority.NORMAL.getPriority(), null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EdmIsShownAt_NullOption() throws ExecutionException {
        ProcessingJobBuilder.edmIsShownAtUrlJobs("", owner, JobPriority.NORMAL.getPriority(), null);
    }

    public void test_EdmObj_InvalidUrl() throws ExecutionException {
        ProcessingJobBuilder.edmObjectUrlJobs(UUID.randomUUID().toString(), owner, JobPriority.NORMAL.getPriority(), falseOption);
    }

    public void test_EdmHasView_InvalidUrl() throws ExecutionException {
        ProcessingJobBuilder.edmHasViewUrlsJobs(Arrays.asList(UUID.randomUUID().toString()), owner, JobPriority.NORMAL.getPriority(), falseOption);
    }

    public void test_EdmIsShownBy_InvalidUrl() throws ExecutionException {
        ProcessingJobBuilder.edmIsShownByUrlJobs(UUID.randomUUID().toString(), owner, JobPriority.NORMAL.getPriority(), falseOption);
    }

    public void test_EdmIsShownAt_InvalidUrl() throws ExecutionException {
        ProcessingJobBuilder.edmIsShownAtUrlJobs(UUID.randomUUID().toString(), owner, JobPriority.NORMAL.getPriority(), falseOption);
    }

    @Test
    public void test_EdmObj_ValidUrl_DefaultOption() throws ExecutionException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmObjectUrlJobs("http://dbooks.bodleian.ox.ac.uk/books/PDFs/590010416.pdf", owner, JobPriority.FASTLANE.getPriority(),
                falseOption);

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 0, 2,
                ProcessingJobSubTaskType.META_EXTRACTION,
                ProcessingJobSubTaskType.COLOR_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL);

    }

    @Test
    public void test_EdmObj_ValidUrl_ConditionalDownload() throws ExecutionException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmObjectUrlJobs("http://dbooks.bodleian.ox.ac.uk/books/PDFs/590010416.pdf", owner, JobPriority.NORMAL.getPriority(),
                falseOption);

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 0, 2,
                ProcessingJobSubTaskType.META_EXTRACTION,
                ProcessingJobSubTaskType.COLOR_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL);
    }

    @Test
    public void testEdmHasViewUrls_OneElement_DefaultOption() throws ExecutionException {
        final List<String> urls = new ArrayList<>();
        urls.addAll(Arrays.asList("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg",
                "https://www.dropbox.com/s/nw9qqf7bk9r79ez/DK_Brendekilde_Udslidt_Brandts.tif?raw=1",
                "http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg"));

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, JobPriority.NORMAL.getPriority(), falseOption);
        assertEquals(urls.size(), jobs.size());

        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2, ProcessingJobSubTaskType.values());
    }



    @Test
    public void testEdmHasViewUrls_TwoElements_DefaultOption() throws ExecutionException {
        final List<String> urls = new ArrayList<>();
        urls.addAll(Arrays.asList("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg",
                "https://www.dropbox.com/s/nw9qqf7bk9r79ez/DK_Brendekilde_Udslidt_Brandts.tif?raw=1",
                "http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg"));


        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, JobPriority.NORMAL.getPriority(), falseOption);

        assertEquals(urls.size(), jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2,
                ProcessingJobSubTaskType.values());

    }

    @Test
    public void testEdmHasViewUrls_ManyElements() throws ExecutionException {
        final List<String> urls = new ArrayList<>();
        urls.addAll(Arrays.asList("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg",
                "https://www.dropbox.com/s/nw9qqf7bk9r79ez/DK_Brendekilde_Udslidt_Brandts.tif?raw=1",
                "http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg"));

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, JobPriority.NORMAL.getPriority(), falseOption);

        assertEquals(urls.size(), jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2, ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmHasViewUrls_OneElement_ForceUnconditionalOption() throws ExecutionException {
        final List<String> urls = new ArrayList<>();
        urls.addAll(Arrays.asList("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg",
                "https://www.dropbox.com/s/nw9qqf7bk9r79ez/DK_Brendekilde_Udslidt_Brandts.tif?raw=1",
                "http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg"));

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, JobPriority.NORMAL.getPriority(), trueOption);
        assertEquals(urls.size(), jobs.size());

        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,  1, 1, 2, ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmHasViewUrls_ManyElements_ForceUnconditionalDownload() throws ExecutionException {
        final List<String> urls = new ArrayList<>();
        urls.addAll(Arrays.asList("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg",
                "https://www.dropbox.com/s/nw9qqf7bk9r79ez/DK_Brendekilde_Udslidt_Brandts.tif?raw=1",
                "http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg"));

        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmHasViewUrlsJobs(urls, owner, JobPriority.NORMAL.getPriority(), trueOption);

        assertEquals(urls.size(), jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 1, 1, 2, ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmIsShownBy_ValidUrl_DefaultOption() throws ExecutionException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownByUrlJobs("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg", owner, JobPriority.NORMAL.getPriority(),
                falseOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, 1, 1, 2,
                ProcessingJobSubTaskType.values());
    }

    @Test
    public void testEdmIsShownBy_ValidUrl_ForceUnconditionalDownload() throws ExecutionException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownByUrlJobs("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg", owner, JobPriority.NORMAL.getPriority(),
                trueOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, 1, 1, 2,
                ProcessingJobSubTaskType.values());
    }


    @Test
    public void testEdmIsShownAt_ValidUrl_DefaultOption() throws ExecutionException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownAtUrlJobs("http://dbooks.bodleian.ox.ac.uk/books/PDFs/590010416.pdf", owner, JobPriority.NORMAL.getPriority(),
                falseOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CHECK_LINK, 0, 0, 0);
    }

    @Test
    public void testEdmIsShownAt_ValidUrl_ForceUnconditionalDownload() throws ExecutionException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmIsShownAtUrlJobs("http://memoriademadrid.es/fondos/OTROS/Imp_34473_mh_1991_1_227_a.jpg", owner, JobPriority.NORMAL.getPriority(), trueOption);
        assertEquals(1, jobs.size());
        validateTasks(jobs, DocumentReferenceTaskType.CHECK_LINK, 0, 0, 0);
    }

    private void validateTasks (List<ProcessingJobTuple> jobs,
                                DocumentReferenceTaskType taskType,
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
