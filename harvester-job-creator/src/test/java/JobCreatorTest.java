import eu.europeana.JobCreator.JobCreator;
import eu.europeana.JobCreator.domain.ProcessingJobCreationOptions;
import eu.europeana.JobCreator.domain.ProcessingJobTuple;
import eu.europeana.harvester.domain.*;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;

public class JobCreatorTest {
    private JobCreator jobCreator;
    private final static String collectionId = "2023831_AG-EU_LinkedHeritage_Rybinsk";
    private final static String providerId = "2023831";
    private final static String recordId = "/2023831/kng_item_item_jsf_id_105436";

    @Before
    public void setUp() {
        jobCreator = new JobCreator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllArgsNull() throws MalformedURLException, UnknownHostException {
        jobCreator.createJobs(null, null, null, null ,null, null, null, null);
    }

    @Test
    public void testEdmObj_ValidUrl() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs =
        jobCreator.createJobs(collectionId, providerId, recordId, "http://www.google.com",
                              null, null, null);

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        final ProcessingJob job = jobs.get(0).getProcessingJob();
        final ProcessingJobTaskDocumentReference jobReference = job.getTasks().get(0);

        assertEquals (JobState.READY, job.getState());
        assertEquals (DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
        assertEquals (3, jobReference.getProcessingTasks().size());

        int countThumbnailTasks = 0;
        boolean hasColourExtractionTask = false, hasThumbnailGeneration = false;

        for (final ProcessingJobSubTask subTask: jobReference.getProcessingTasks()) {
            if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                hasColourExtractionTask = true;
            }
            else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                hasThumbnailGeneration = true;
                ++countThumbnailTasks;
            }
            else {
                fail ("Generated wrong subtask type: " + subTask.getTaskType().name());
            }
        }

        assertEquals (2, countThumbnailTasks);
        assertTrue (hasColourExtractionTask);
        assertTrue (hasThumbnailGeneration);
    }

    @Test
    public void testEdmObj_ValidUrl_UnconditionDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, "http://www.google.com",
                                      null, null, null, new ProcessingJobCreationOptions(true));

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        final ProcessingJob job = jobs.get(0).getProcessingJob();
        final ProcessingJobTaskDocumentReference jobReference = job.getTasks().get(0);

        assertEquals (JobState.READY, job.getState());
        assertEquals (DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, jobReference.getTaskType());
        assertEquals (3, jobReference.getProcessingTasks().size());

        int countThumbnailTasks = 0;
        boolean hasColourExtractionTask = false, hasThumbnailGeneration = false;

        for (final ProcessingJobSubTask subTask: jobReference.getProcessingTasks()) {
            if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                hasColourExtractionTask = true;
            }
            else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                hasThumbnailGeneration = true;
                ++countThumbnailTasks;
            }
            else {
                fail ("Generated wrong subtask type: " + subTask.getTaskType().name());
            }
        }

        assertEquals (2, countThumbnailTasks);
        assertTrue (hasColourExtractionTask);
        assertTrue (hasThumbnailGeneration);
    }

    @Test(expected = MalformedURLException.class)
    public void testEdmObj_InvalidUrl() throws MalformedURLException, UnknownHostException {
        jobCreator.createJobs(collectionId, providerId, recordId, "img.jgi.doe.gov/ges/img-user-m.p", null, null, null, null);
    }

    @Test
    public void testEdmHasViewUrls_OneElement() throws MalformedURLException, UnknownHostException   {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");

        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, urls, null, null, new ProcessingJobCreationOptions(true));

        assertEquals(urls.size(), jobs.size());

        for (final ProcessingJobTuple job: jobs) {
            assertEquals(1, job.getProcessingJob().getTasks().size());

            final ProcessingJobTaskDocumentReference jobReference = job.getProcessingJob().getTasks().get(0);

            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
            assertEquals(4, jobReference.getProcessingTasks().size());

            int countThumbnailTasks = 0;
            boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

            for (final ProcessingJobSubTask subTask : jobReference.getProcessingTasks()) {
                if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                    hasColourExtractionTask = true;
                }
                else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                    hasThumbnailGeneration = true;
                    ++countThumbnailTasks;
                }
                else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                    hasMetadataExtraction = true;
                }
                else {
                    fail("Generated wrong subtask type: " + subTask.getTaskType().name());
                }
            }

            assertEquals(2, countThumbnailTasks);
            assertTrue(hasColourExtractionTask);
            assertTrue(hasThumbnailGeneration);
            assertTrue(hasMetadataExtraction);
        }
    }

    @Test
    public void testEdmHasViewUrls_TwoElements()throws MalformedURLException, UnknownHostException  {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");
        urls.add("http://www.facebook.com");

        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, urls, null, null, new ProcessingJobCreationOptions(true));

        assertEquals(urls.size(), jobs.size());

        for (final ProcessingJobTuple job: jobs) {
            assertEquals(1, job.getProcessingJob().getTasks().size());

            final ProcessingJobTaskDocumentReference jobReference = job.getProcessingJob().getTasks().get(0);

            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, jobReference.getTaskType());
            assertEquals(4, jobReference.getProcessingTasks().size());

            int countThumbnailTasks = 0;
            boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

            for (final ProcessingJobSubTask subTask : jobReference.getProcessingTasks()) {
                if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                    hasColourExtractionTask = true;
                }
                else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                    hasThumbnailGeneration = true;
                    ++countThumbnailTasks;
                }
                else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                    hasMetadataExtraction = true;
                }
                else {
                    fail("Generated wrong subtask type: " + subTask.getTaskType().name());
                }
            }

            assertEquals(2, countThumbnailTasks);
            assertTrue(hasColourExtractionTask);
            assertTrue(hasThumbnailGeneration);
            assertTrue(hasMetadataExtraction);
        }
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


        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, urls, null, null, new ProcessingJobCreationOptions(false));

        assertEquals(urls.size(), jobs.size());

        for (final ProcessingJobTuple job: jobs) {
            assertEquals(1, job.getProcessingJob().getTasks().size());

            final ProcessingJobTaskDocumentReference jobReference = job.getProcessingJob().getTasks().get(0);

            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
            assertEquals(4, jobReference.getProcessingTasks().size());

            int countThumbnailTasks = 0;
            boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

            for (final ProcessingJobSubTask subTask : jobReference.getProcessingTasks()) {
                if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                    hasColourExtractionTask = true;
                }
                else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                    hasThumbnailGeneration = true;
                    ++countThumbnailTasks;
                }
                else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                    hasMetadataExtraction = true;
                }
                else {
                    fail("Generated wrong subtask type: " + subTask.getTaskType().name());
                }
            }

            assertEquals(2, countThumbnailTasks);
            assertTrue(hasColourExtractionTask);
            assertTrue(hasThumbnailGeneration);
            assertTrue(hasMetadataExtraction);
        }
    }

    @Test(expected = MalformedURLException.class)
    public void testEdmHasViewUrls_OneInvalidUrl()throws MalformedURLException, UnknownHostException  {
        final List<String> urls = new ArrayList<>();
        urls.add(UUID.randomUUID().toString());

        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, urls, null, null, new ProcessingJobCreationOptions(false));

        assertEquals(urls.size(), jobs.size());

        for (final ProcessingJobTuple job: jobs) {
            assertEquals(1, job.getProcessingJob().getTasks().size());

            final ProcessingJobTaskDocumentReference jobReference = job.getProcessingJob().getTasks().get(0);

            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
            assertEquals(4, jobReference.getProcessingTasks().size());

            int countThumbnailTasks = 0;
            boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

            for (final ProcessingJobSubTask subTask : jobReference.getProcessingTasks()) {
                if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                    hasColourExtractionTask = true;
                }
                else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                    hasThumbnailGeneration = true;
                    ++countThumbnailTasks;
                }
                else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                    hasMetadataExtraction = true;
                }
                else {
                    fail("Generated wrong subtask type: " + subTask.getTaskType().name());
                }
            }

            assertEquals(2, countThumbnailTasks);
            assertTrue(hasColourExtractionTask);
            assertTrue(hasThumbnailGeneration);
            assertTrue(hasMetadataExtraction);
        }
    }

    @Test(expected = MalformedURLException.class)
    public void testEdmHasView_Urls_ManyElementsSomeInvalidUrls() throws MalformedURLException, UnknownHostException {
        final List<String> urls = new ArrayList<>();
        urls.add("http://www.google.com");
        urls.add("http://www.facebook.com");
        urls.add("http://www.yahoo.com");
        urls.add(UUID.randomUUID().toString());
        urls.add("http://http://www.wikipedia.org");
        urls.add("http://de.wikipedia.org/wiki");
        urls.add("http://www.w3schools.com/");
        urls.add(UUID.randomUUID().toString());
        urls.add("http://www.skype.com");
        urls.add(UUID.randomUUID().toString());

        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, urls, null, null, new ProcessingJobCreationOptions(false));

        assertEquals(urls.size(), jobs.size());

        for (final ProcessingJobTuple job: jobs) {
            assertEquals(1, job.getProcessingJob().getTasks().size());

            final ProcessingJobTaskDocumentReference jobReference = job.getProcessingJob().getTasks().get(0);

            assertEquals(JobState.READY, job.getProcessingJob().getState());
            assertEquals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
            assertEquals(4, jobReference.getProcessingTasks().size());

            int countThumbnailTasks = 0;
            boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

            for (final ProcessingJobSubTask subTask : jobReference.getProcessingTasks()) {
                if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                    hasColourExtractionTask = true;
                }
                else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                    hasThumbnailGeneration = true;
                    ++countThumbnailTasks;
                }
                else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                    hasMetadataExtraction = true;
                }
                else {
                    fail("Generated wrong subtask type: " + subTask.getTaskType().name());
                }
            }

            assertEquals(2, countThumbnailTasks);
            assertTrue(hasColourExtractionTask);
            assertTrue(hasThumbnailGeneration);
            assertTrue(hasMetadataExtraction);
        }
    }

    @Test
    public void testEdmIsShownBy_ValidUrl() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, null, "http://www.google.com", null, new ProcessingJobCreationOptions(false));

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        final ProcessingJob job = jobs.get(0).getProcessingJob();
        final ProcessingJobTaskDocumentReference jobReference = job.getTasks().get(0);

        assertEquals(JobState.READY, job.getState());
        assertEquals(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
        assertEquals(4, jobReference.getProcessingTasks().size());

        int countThumbnailTasks = 0;
        boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

        for (final ProcessingJobSubTask subTask: jobReference.getProcessingTasks()) {
            if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                hasColourExtractionTask = true;
            }
            else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                hasThumbnailGeneration = true;
                ++countThumbnailTasks;
            }
            else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                hasMetadataExtraction = true;
            }
            else {
                fail ("Generated wrong subtask type: " + subTask.getTaskType().name());
            }
        }

        assertEquals(2, countThumbnailTasks);
        assertTrue(hasColourExtractionTask);
        assertTrue(hasThumbnailGeneration);
        assertTrue(hasMetadataExtraction);
    }

    @Test
    public void testEdmIsShownBy_ValidUrl_UnconditionalDownload() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, null, "http://www.google.com", null, new ProcessingJobCreationOptions(true));

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        final ProcessingJob job = jobs.get(0).getProcessingJob();
        final ProcessingJobTaskDocumentReference jobReference = job.getTasks().get(0);

        assertEquals (JobState.READY, job.getState());
        assertEquals (DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, jobReference.getTaskType());
        assertEquals (4, jobReference.getProcessingTasks().size());

        int countThumbnailTasks = 0;
        boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

        for (final ProcessingJobSubTask subTask: jobReference.getProcessingTasks()) {
            if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                hasColourExtractionTask = true;
            }
            else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                hasThumbnailGeneration = true;
                ++countThumbnailTasks;
            }
            else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                hasMetadataExtraction = true;
            }
            else {
                fail ("Generated wrong subtask type: " + subTask.getTaskType().name());
            }
        }

        assertEquals (2, countThumbnailTasks);
        assertTrue(hasColourExtractionTask);
        assertTrue (hasThumbnailGeneration);
        assertTrue (hasMetadataExtraction);
    }

    @Test
    public void testEdmIsShownBy_ValidUrl_NoneOptions() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, null, "http://www.google.com", null, new ProcessingJobCreationOptions(false));

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        final ProcessingJob job = jobs.get(0).getProcessingJob();
        final ProcessingJobTaskDocumentReference jobReference = job.getTasks().get(0);

        assertEquals (JobState.READY, job.getState());
        assertEquals (DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, jobReference.getTaskType());
        assertEquals (4, jobReference.getProcessingTasks().size());

        int countThumbnailTasks = 0;
        boolean hasColourExtractionTask = false, hasThumbnailGeneration = false, hasMetadataExtraction = false;

        for (final ProcessingJobSubTask subTask: jobReference.getProcessingTasks()) {
            if (ProcessingJobSubTaskType.COLOR_EXTRACTION == subTask.getTaskType()) {
                hasColourExtractionTask = true;
            }
            else if (ProcessingJobSubTaskType.GENERATE_THUMBNAIL == subTask.getTaskType()) {
                hasThumbnailGeneration = true;
                ++countThumbnailTasks;
            }
            else if (ProcessingJobSubTaskType.META_EXTRACTION == subTask.getTaskType()) {
                hasMetadataExtraction = true;
            }
            else {
                fail ("Generated wrong subtask type: " + subTask.getTaskType().name());
            }
        }

        assertEquals (2, countThumbnailTasks);
        assertTrue(hasColourExtractionTask);
        assertTrue (hasThumbnailGeneration);
        assertTrue (hasMetadataExtraction);
    }

    @Test(expected = MalformedURLException.class)
    public void testEdmIsShownBy_InvalidUrl() throws MalformedURLException, UnknownHostException  {
        jobCreator.createJobs(collectionId, providerId, recordId, null, null, UUID.randomUUID().toString(), null, null);
    }

    @Test
    public void testEdmIsShownAt_ValidUrl() throws MalformedURLException, UnknownHostException  {
        final List<ProcessingJobTuple> jobs =
                jobCreator.createJobs(collectionId, providerId, recordId, null, null, null, "http://www.google.com", new ProcessingJobCreationOptions(false));

        assertEquals(1, jobs.size());
        assertEquals(1, jobs.get(0).getProcessingJob().getTasks().size());

        final ProcessingJob job = jobs.get(0).getProcessingJob();
        final ProcessingJobTaskDocumentReference jobReference = job.getTasks().get(0);

        assertEquals(JobState.READY, job.getState());
        assertEquals(DocumentReferenceTaskType.CHECK_LINK, jobReference.getTaskType());
        assertEquals(0, jobReference.getProcessingTasks().size());
    }

    @Test(expected = MalformedURLException.class)
    public void testEdmIsShownAt_InvalidUrl() throws MalformedURLException, UnknownHostException {
        jobCreator.createJobs(collectionId, providerId, recordId, null, null, null, UUID.randomUUID().toString(), null);
    }

}
