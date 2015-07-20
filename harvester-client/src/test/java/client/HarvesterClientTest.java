package client;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Created by salexandru on 10.06.2015.
 */
public class HarvesterClientTest {
    private HarvesterClient harvesterClient;
    private SourceDocumentReferenceDao SourceDocumentReferenceDao;
    private SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;
    private SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;
    private MachineResourceReferenceDao machineResourceReferenceDao;
    private ProcessingJobDao processingJobDao;
    private SourceDocumentReferenceProcessingProfileDao sourceDocumentReferenceProcessingProfileDao;


    private Datastore datastore;


    @Before
    public void setUp() throws UnknownHostException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        Morphia morphia = new Morphia();
        String dbName = "europeana";

        datastore = morphia.createDatastore(mongo, dbName);

        SourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        sourceDocumentProcessingStatisticsDao = new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);
        machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        processingJobDao = new ProcessingJobDaoImpl(datastore);
        sourceDocumentReferenceProcessingProfileDao = new SourceDocumentReferenceProcessingProfileDaoImpl(datastore);

        harvesterClient = new HarvesterClientImpl(processingJobDao,
                                                  machineResourceReferenceDao,
                                                  sourceDocumentProcessingStatisticsDao, SourceDocumentReferenceDao,
                                                  sourceDocumentReferenceMetaInfoDao,
                                                  sourceDocumentReferenceProcessingProfileDao,
                                                  new HarvesterClientConfig(WriteConcern.ACKNOWLEDGED)
        );
    }

    @After
    public void tearDown() {
        datastore.delete(datastore.createQuery(ProcessingJob.class));
        datastore.delete(datastore.createQuery(MachineResourceReference.class));
        datastore.delete(datastore.createQuery(SourceDocumentProcessingStatistics.class));
        datastore.delete(datastore.createQuery(SourceDocumentReferenceMetaInfo.class));
        datastore.delete(datastore.createQuery(SourceDocumentReference.class));
        datastore.delete(datastore.createQuery(SourceDocumentReferenceProcessingProfile.class));
    }

    @Test
    public void test_CreateOrModifySourceDocumentReference_NullCollection() throws InterruptedException,
                                                                                   MalformedURLException,
                                                                                   TimeoutException, ExecutionException,
                                                                                   UnknownHostException {
        assertFalse(harvesterClient.createOrModifySourceDocumentReference(null).iterator().hasNext());
    }

    @Test
    public void test_CreateOrModifySourceDocumentReference_EmptyCollection() throws InterruptedException,
                                                                                    MalformedURLException,
                                                                                    TimeoutException,
                                                                                    ExecutionException,
                                                                                    UnknownHostException {
        assertFalse(harvesterClient.createOrModifySourceDocumentReference(Collections.EMPTY_LIST).iterator().hasNext());
    }

    @Test
    public void test_CreateSourceDocumentReferences_Many() throws InterruptedException, MalformedURLException,
                                                                  TimeoutException, ExecutionException,
                                                                  UnknownHostException {
        final SourceDocumentReference[] sourceDocumentReferences = new SourceDocumentReference[50];
        final Random random = new Random();

        for (int i = 0; i < sourceDocumentReferences.length; ++i) {
            final String iString = Integer.toString(i);
            sourceDocumentReferences[i] = new SourceDocumentReference(
                iString,
                new ReferenceOwner(iString, iString, iString, iString),
                "http://www.google.com",
                "127.0.0.1",
                "",
                0L,
                null,
                false
            );
        }

        harvesterClient.createOrModifySourceDocumentReference(Arrays.asList(sourceDocumentReferences));

        for (final SourceDocumentReference reference: sourceDocumentReferences) {
            final SourceDocumentReference writtenReference = SourceDocumentReferenceDao.read(reference.getId());

            ReflectionAssert.assertReflectionEquals(reference, writtenReference);
        }
    }

    @Test
    public void test_ModifySourceDocumentReferences_Many() throws InterruptedException, MalformedURLException,
                                                                  TimeoutException, ExecutionException,
                                                                  UnknownHostException {
        final SourceDocumentReference[] sourceDocumentReferences = new SourceDocumentReference[50];
        final Random random = new Random();

        for (int i = 0; i < sourceDocumentReferences.length; ++i) {
            final String iString = Integer.toString(i);
            sourceDocumentReferences[i] = new SourceDocumentReference(
                                                                             iString,
                                                                             new ReferenceOwner(iString, iString, iString, iString),
                                                                             "http://www.google.com",
                                                                             "127.0.0.1",
                                                                             "",
                                                                             0L,
                                                                             null,
                                                                             false
            );
        }

        harvesterClient.createOrModifySourceDocumentReference(Arrays.asList(sourceDocumentReferences));

        for (int i = 0; i < sourceDocumentReferences.length; ++i) {
            if (random.nextBoolean()) {
                final String iString = Integer.toString(i);
                sourceDocumentReferences[i] = new SourceDocumentReference(
                                                                                 iString,
                                                                                 new ReferenceOwner(iString, iString, iString, ""),
                                                                                 "http://www.skype.com",
                                                                                 "127.1.1.1",
                                                                                 "",
                                                                                 0L,
                                                                                 null,
                                                                                 true
                );
            }
        }

        harvesterClient.createOrModifySourceDocumentReference(Arrays.asList(sourceDocumentReferences));

        for (final SourceDocumentReference reference: sourceDocumentReferences) {
            final SourceDocumentReference writtenReference = SourceDocumentReferenceDao.read(reference.getId());

            ReflectionAssert.assertReflectionEquals(reference, writtenReference);
        }
    }

    @Test
    public void test_DeactivateJobs() {
        final ReferenceOwner[] owners = new ReferenceOwner[] {
            new ReferenceOwner("1", "1", "1"),
            new ReferenceOwner("2", "1", "1", "2"),
            new ReferenceOwner("1", "1", "2", "1")
        };

        final Map<ReferenceOwner, Set<String>> processingJobIds = new HashMap<>();
        final Map<ReferenceOwner, Set<String>> sourceDocumentReferenceIds = new HashMap<>();
        final Map<ReferenceOwner, Set<String>> sourceDocumentProcessingStatisticsIds = new HashMap<>();
        final Map<ReferenceOwner, Set<String>> sourceDocumentProcessingProfileIds = new HashMap<>();

        final Random random = new Random(System.nanoTime());

        for (final ReferenceOwner owner: owners) {
            processingJobIds.put(owner, new HashSet<String>());
            sourceDocumentReferenceIds.put(owner, new HashSet<String>());
            sourceDocumentProcessingStatisticsIds.put(owner, new HashSet<String>());
            sourceDocumentProcessingProfileIds.put(owner, new HashSet<String>());
        }

        for (int i = 0; i < 150; ++i) {
            final String id = UUID.randomUUID().toString();
            final ReferenceOwner owner = owners[random.nextInt(3)];

            processingJobIds.get(owner).add(id);

            final ProcessingJob processingJob =
                    new ProcessingJob(id, 1, new Date(), owner, null, JobState.READY, URLSourceType.HASVIEW, "", null, null);



            final SourceDocumentReference sourceDocumentReference =
                    new SourceDocumentReference(owner, "test", null, null, 0l, null, true);



            sourceDocumentReferenceIds.get(owner).add(sourceDocumentReference.getId());

            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    new SourceDocumentProcessingStatistics(new Date(), new Date(), true, null, null, owner,
                                                           null, sourceDocumentReference.getId(), "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "");


            sourceDocumentProcessingStatisticsIds.get(owner).add(sourceDocumentProcessingStatistics.getId());

            final SourceDocumentReferenceProcessingProfile profile =
                    new SourceDocumentReferenceProcessingProfile(true,
                                                                 owner,
                                                                 sourceDocumentReference.getId(),
                                                                 URLSourceType.ISSHOWNAT,
                                                                 DocumentReferenceTaskType.CHECK_LINK,
                                                                 0,
                                                                 new Date(),
                                                                 10);
            sourceDocumentProcessingProfileIds.get(owner).add(profile.getId());

            SourceDocumentReferenceDao.create(sourceDocumentReference, WriteConcern.NONE);
            processingJobDao.create(processingJob, WriteConcern.NONE);
            sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);
            sourceDocumentReferenceProcessingProfileDao.create(profile, WriteConcern.NONE);
        }

        final List<ProcessingJob> jobs = harvesterClient.deactivateJobs(owners[1]);

        assertEquals(processingJobIds.get(owners[1]).size(), jobs.size());
        for (final String jobId: processingJobIds.get(owners[1])) {
           assertFalse(processingJobDao.read(jobId).getActive());
        }

        for (final String referenceId: sourceDocumentReferenceIds.get(owners[1])) {
            assertFalse(SourceDocumentReferenceDao.read(referenceId).getActive());
        }

        for (final String id: sourceDocumentProcessingStatisticsIds.get(owners[1])) {
            assertFalse(sourceDocumentProcessingStatisticsDao.read(id).getActive());
        }

        for (final String id: sourceDocumentProcessingProfileIds.get(owners[1])) {
            assertFalse(sourceDocumentReferenceProcessingProfileDao.read(id).getActive());
        }
    }

}
