package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.domain.*;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

public class ProcessingJobDaoImplTest {

    private ProcessingJobDao processingJobDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "europeana";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        processingJobDao = new ProcessingJobDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "a.com", null, null, 0l, null);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        sourceDocumentReference.getId());
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<ProcessingJobTaskDocumentReference>();
        tasks.add(processingJobTaskDocumentReference);
        final ProcessingJob processingJob =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"), tasks, JobState.READY);

        processingJobDao.create(processingJob);
        assertEquals(processingJob.getId(), processingJobDao.read(processingJob.getId()).getId());

        processingJobDao.delete(processingJob);
    }

    @Test
    public void testRead() throws Exception {
        ProcessingJob processingJobFromRead = processingJobDao.read("");
        assertNull(processingJobFromRead);

        final String startDateString = "2013-03-26";
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        final Date date = df.parse(startDateString);

        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "a.com", null, null, 0l, null);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
            new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                    sourceDocumentReference.getId());
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<ProcessingJobTaskDocumentReference>();
        tasks.add(processingJobTaskDocumentReference);

        final ProcessingJob processingJob =
                new ProcessingJob(date, new ReferenceOwner("1", "1", "1"), tasks, JobState.READY);
        processingJobDao.create(processingJob);

        assertEquals(processingJob.getExpectedStartDate(),
                processingJobDao.read(processingJob.getId()).getExpectedStartDate());

        processingJobDao.delete(processingJob);
    }

    @Test
    public void testUpdate() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY);
        processingJobDao.create(processingJob);

        final ProcessingJob updatedProcessingJob = processingJob.withState(JobState.FINISHED);

        assertNotEquals(updatedProcessingJob.getState(), processingJobDao.read(processingJob.getId()).getState());
        assertTrue(processingJobDao.update(updatedProcessingJob));
        assertEquals(updatedProcessingJob.getState(), processingJobDao.read(processingJob.getId()).getState());

        processingJobDao.delete(updatedProcessingJob);

        final ProcessingJob newProcessingJob =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY);
        assertFalse(processingJobDao.update(newProcessingJob));
    }

    @Test
    public void testDelete() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY);
        processingJobDao.create(processingJob);

        ProcessingJob processingJobFromRead = processingJobDao.read(processingJob.getId());
        assertNotNull(processingJobFromRead);

        assertTrue(processingJobDao.delete(processingJob));

        processingJobFromRead = processingJobDao.read(processingJob.getId());
        assertNull(processingJobFromRead);

        assertFalse(processingJobDao.delete(processingJob));
    }

    @Test
    public void testGetAllJobs() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY);
        processingJobDao.create(processingJob);

        List<ProcessingJob> allJobs = processingJobDao.getAllJobs();
        assertNotNull(allJobs);

        processingJobDao.delete(processingJob);
    }

}
