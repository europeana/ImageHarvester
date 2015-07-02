package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.domain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

public class ProcessingJobDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(ProcessingJobDaoImplTest.class.getName());

    private ProcessingJobDao processingJobDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "harvester_persistency";

            String username = "harvester_persistency";
            String password = "Nhck0zCfcu0M6kK";

            boolean auth = mongo.getDB("admin").authenticate(username, password.toCharArray());

            if (!auth) {
                fail("couldn't authenticate " + username + " against admin db");
            }

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        processingJobDao = new ProcessingJobDaoImpl(datastore);
    }

    @Test
    public void testCreate_NullCollection() {
        assertFalse(processingJobDao.createOrModify((Collection)null, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void testCreate_EmptyCollection() {
        assertFalse(processingJobDao.createOrModify(Collections.EMPTY_LIST, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void testCreate_OneElement() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "a.com", null, null, 0l, null, true);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        sourceDocumentReference.getId(), null);
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<ProcessingJobTaskDocumentReference>();
        tasks.add(processingJobTaskDocumentReference);
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), tasks, JobState.READY, "");

        processingJobDao.create(processingJob, WriteConcern.NONE);
        assertEquals(processingJob.getId(), processingJobDao.read(processingJob.getId()).getId());

        processingJobDao.delete(processingJob.getId());
    }

    @Test
    public void testCreateOrModify_ManyElements() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "a.com", null, null, 0l, null, true);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                                       sourceDocumentReference.getId(), null);
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<>();
        tasks.add(processingJobTaskDocumentReference);

        final List<ProcessingJob> processingJobs = new ArrayList<>();
        final Random random = new Random();
        for (int i = 0; i < 50; ++i) {
            processingJobs.add(new ProcessingJob(
                  1,
                  DateTime.now().toDate(),
                  new ReferenceOwner("1", "1", "1"),
                  tasks,
                  JobState.values()[random.nextInt(JobState.values().length)],
                  "127.0.0.1"
            ));
        }

        processingJobDao.createOrModify(processingJobs, WriteConcern.NONE);

        for (final ProcessingJob job: processingJobs) {
            final ProcessingJob writtenJob = processingJobDao.read(job.getId());


            processingJobDao.delete(job.getId());
            ReflectionAssert.assertReflectionEquals(job, writtenJob);
        }
    }

    @Test
    public void testRead() throws Exception {
        ProcessingJob processingJobFromRead = processingJobDao.read("");
        assertNull(processingJobFromRead);

        final String startDateString = "2013-03-26";
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

        final Date date = df.parse(startDateString);

        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "a.com", null, null, 0l, null, true);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
            new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                    sourceDocumentReference.getId(), null);
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<ProcessingJobTaskDocumentReference>();
        tasks.add(processingJobTaskDocumentReference);

        final ProcessingJob processingJob =
                new ProcessingJob(1, date, new ReferenceOwner("1", "1", "1"), tasks, JobState.READY, "");
        processingJobDao.create(processingJob, WriteConcern.NONE);

        assertEquals(processingJob.getExpectedStartDate(),
                processingJobDao.read(processingJob.getId()).getExpectedStartDate());

        processingJobDao.delete(processingJob.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, "");
        processingJobDao.create(processingJob, WriteConcern.NONE);

        final ProcessingJob updatedProcessingJob = processingJob.withState(JobState.FINISHED);

        assertNotEquals(updatedProcessingJob.getState(), processingJobDao.read(processingJob.getId()).getState());
        assertTrue(processingJobDao.update(updatedProcessingJob, WriteConcern.NONE));
        assertEquals(updatedProcessingJob.getState(), processingJobDao.read(processingJob.getId()).getState());

        processingJobDao.delete(updatedProcessingJob.getId());

        final ProcessingJob newProcessingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, "");
        assertFalse(processingJobDao.update(newProcessingJob, WriteConcern.NONE));
    }

    @Test
    public void testDelete() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, "");
        processingJobDao.create(processingJob, WriteConcern.NONE);

        ProcessingJob processingJobFromRead = processingJobDao.read(processingJob.getId());
        assertNotNull(processingJobFromRead);

        assertTrue(processingJobDao.delete(processingJob.getId()).getN() == 1);

        processingJobFromRead = processingJobDao.read(processingJob.getId());
        assertNull(processingJobFromRead);

        assertFalse(processingJobDao.delete(processingJob.getId()).getN() == 1);
    }

    @Test
    public void testGetJobsWithState() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, "");
        processingJobDao.create(processingJob, WriteConcern.NONE);

        List<ProcessingJob> allJobs = processingJobDao.getJobsWithState(JobState.READY, new Page(0, 1));
        assertNotNull(allJobs);

        processingJobDao.delete(processingJob.getId());
    }

}
