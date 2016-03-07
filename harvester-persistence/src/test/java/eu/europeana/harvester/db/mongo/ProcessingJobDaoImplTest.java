package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.util.pagedElements.PagedElements;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.*;

public class ProcessingJobDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(ProcessingJobDaoImplTest.class.getName());

    private ProcessingJobDao processingJobDao;
    private List<String> ids;


    private MongodProcess mongod = null;
    private MongodExecutable mongodExecutable = null;
    private int port = 12345;

    public ProcessingJobDaoImplTest() throws IOException {

        MongodStarter starter = MongodStarter.getDefaultInstance();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(port, Network.localhostIsIPv6()))
                .build();

        mongodExecutable = starter.prepare(mongodConfig);
    }

    @Before
    public void setUp() throws Exception {
        mongod = mongodExecutable.start();

        Datastore datastore = null;
        ids = new ArrayList<>();

        try {
            MongoClient mongo = new MongoClient("localhost", port);
            Morphia morphia = new Morphia();
            String dbName = "harvester_persistency";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        processingJobDao = new ProcessingJobDaoImpl(datastore);
    }

    @After
    public void tearDown() {
        for (final String id: ids) {
            processingJobDao.delete(id);
        }
        mongodExecutable.stop();
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
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "a.com", null, null, 0l, null, true);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        sourceDocumentReference.getId(), null);
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<ProcessingJobTaskDocumentReference>();
        tasks.add(processingJobTaskDocumentReference);
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), tasks, JobState.READY, null, "", null);

        processingJobDao.create(processingJob, WriteConcern.NONE);
        assertEquals(processingJob.getId(), processingJobDao.read(processingJob.getId()).getId());

        processingJobDao.delete(processingJob.getId());
    }

    @Test
    public void testCreateOrModify_ManyElements() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"),  "a.com", null, null, 0l, null, true);
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
                  null,
                  "127.0.0.1", null));
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
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "a.com", null, null, 0l, null, true);
        final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
            new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                    sourceDocumentReference.getId(), null);
        final List<ProcessingJobTaskDocumentReference> tasks = new ArrayList<ProcessingJobTaskDocumentReference>();
        tasks.add(processingJobTaskDocumentReference);

        final ProcessingJob processingJob =
                new ProcessingJob(1, date, new ReferenceOwner("1", "1", "1"), tasks, JobState.READY, null, "", null);
        processingJobDao.create(processingJob, WriteConcern.NONE);

        assertEquals(processingJob.getExpectedStartDate(),
                processingJobDao.read(processingJob.getId()).getExpectedStartDate());

        processingJobDao.delete(processingJob.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, null, "", null);
        processingJobDao.create(processingJob, WriteConcern.NONE);

        final ProcessingJob updatedProcessingJob = processingJob.withState(JobState.FINISHED);

        assertNotEquals(updatedProcessingJob.getState(), processingJobDao.read(processingJob.getId()).getState());
        assertTrue(processingJobDao.update(updatedProcessingJob, WriteConcern.NONE));
        assertEquals(updatedProcessingJob.getState(), processingJobDao.read(processingJob.getId()).getState());

        processingJobDao.delete(updatedProcessingJob.getId());

        final ProcessingJob newProcessingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, null, "", null);
        assertFalse(processingJobDao.update(newProcessingJob, WriteConcern.NONE));
    }

    @Test
    public void testDelete() throws Exception {
        final ProcessingJob processingJob =
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, null, "", null);
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
                new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"), null, JobState.READY, null, "", null);
        processingJobDao.create(processingJob, WriteConcern.NONE);

        List<ProcessingJob> allJobs = processingJobDao.getJobsWithState(JobState.READY, new Page(0, 1));
        assertNotNull(allJobs);

        processingJobDao.delete(processingJob.getId());
    }

    @Test
    public void testUpdateDocuments() throws Exception {
        final ReferenceOwner[] referenceOwners = new ReferenceOwner[] {new ReferenceOwner("1", "1", "1", "1"),
                                                                       new ReferenceOwner("2", "1", "2", "3")
                                                                      };
        final Random random = new Random();
        for (int i = 0; i < 50; ++i) {
            final String id = UUID.randomUUID().toString();
            ids.add(id);
            final ProcessingJob processingJob =
                    new ProcessingJob(id, 1, new Date(), referenceOwners[random.nextInt(2)], null, JobState.READY, null, "",
                                      null, null);

            processingJobDao.create(processingJob, WriteConcern.NONE);
        }

        for (final ProcessingJob job: processingJobDao.deactivateJobs(referenceOwners[0], WriteConcern.ACKNOWLEDGED)) {
            assertFalse (job.getActive());
            assertFalse (processingJobDao.read(job.getId()).getActive());
        }
    }

    @Test
    public void testFindJobsByCollectionIdAndState_CorrectElements() throws Exception {
        final JobState[] jobStates = JobState.values();
        int count = 0;
        final Set<String> correctIds = new HashSet<>();
        for (int i = 0; i < 500; ++i) {
            final ReferenceOwner owner = new ReferenceOwner("1", "1", "1");
            final ProcessingJob p =
                    new ProcessingJob(
                            Integer.toString(i),
                            1,
                            new Date(),
                            owner,
                            null,
                            jobStates[i % jobStates.length],
                            URLSourceType.HASVIEW,
                            "",
                            false,
                            new ProcessingJobLimits(10L, 10L, 10L, 10, 10L)
                    );

            if (p.getState() == JobState.FINISHED || p.getState() == JobState.READY) {
                ++count;
                correctIds.add(Integer.toString(i));
            }
            processingJobDao.create(p, WriteConcern.NORMAL);
        }

        for (int i = 0; i < 1000; ++i) {
            final ReferenceOwner owner = new ReferenceOwner(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
            final ProcessingJob p =
                    new ProcessingJob(1, new Date(), owner, null, jobStates[i % jobStates.length], null, "", null);
            processingJobDao.create(p, WriteConcern.NORMAL);
        }

        final Set<String> collectionIds = new HashSet<>();
        collectionIds.add("1");
        final Set<JobState> jobStateValues = new HashSet<>();
        jobStateValues.add(JobState.FINISHED);
        jobStateValues.add(JobState.READY);

        System.out.println(correctIds.toString());

        {
            final Set<String> correctIds_ = new HashSet<>(correctIds);
            final PagedElements<ProcessingJob> paged = processingJobDao.findJobsByCollectionIdAndState(collectionIds, jobStateValues, new Page(0, 20));

            while (paged.hasNext()) {
                final List<ProcessingJob> elements = paged.getNextPage();

                if (0 != (count % 20) && elements.size() < 20) {
                    assertFalse(paged.hasNext());
                    assert (elements.size() == count % 20);
                } else {
                    assert (elements.size() == 20);
                }
                for (final ProcessingJob elem : elements) {
                    System.out.println(elem.getId());
                    assertTrue(correctIds_.contains(elem.getId()));
                    correctIds_.remove(elem.getId());
                }
            }
            assertFalse(paged.hasNext());
        }
        {
            final Set<String> correctIds_ = new HashSet<>(correctIds);
            final PagedElements<ProcessingJob> paged = processingJobDao.findJobsByCollectionIdAndState(collectionIds, jobStateValues, new Page(0, 20));

            for (final List<ProcessingJob> elements: paged) {
                if (0 != (count % 20) && elements.size() <  20) {
                    assertFalse(paged.hasNext());
                    assert (elements.size() == count % 20);
                }
                else {
                    assert (elements.size() == 20);
                }
                for (final ProcessingJob elem: elements) {
                    System.out.println(elem.getId()); System.out.flush();
                    assertTrue (correctIds_.contains(elem.getId()));
                    correctIds_.remove(elem.getId());
                }
            }
            assertFalse(paged.hasNext());
        }

    }

}
