package eu.europeana.harvester.cluster.master.jobrestarter;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.jobcreator.logic.SubTaskBuilder;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by salexandru on 20.07.2015.
 */
public class JobRestarterHelperTest {
    private SourceDocumentReferenceDao sourceDocumentReferenceDao;
    private ProcessingJobDao processingJobDao;
    private SourceDocumentReferenceProcessingProfileDao sourceDocumentReferenceProcessingProfileDao;


    private Datastore datastore;

    private JobRestarterHelper helper;


    @Before
    public void setUp() throws UnknownHostException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        Morphia morphia = new Morphia();
        String dbName = "crf_europeana_jobRestarter";

        final String username = "crf_europeana_jobRestarter";
        final String password = "Nhck0zCfcu0M6kK";

        if (!mongo.getDB("admin").authenticate(username, password.toCharArray())) {
            fail ("Couldn't auth info db");
        }

        datastore = morphia.createDatastore(mongo, dbName);

        sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        processingJobDao = new ProcessingJobDaoImpl(datastore);
        sourceDocumentReferenceProcessingProfileDao = new SourceDocumentReferenceProcessingProfileDaoImpl(datastore);

        helper = new JobRestarterHelper(sourceDocumentReferenceDao, processingJobDao, sourceDocumentReferenceProcessingProfileDao);
    }

    @After
    public void tearDown() {
        datastore.delete(datastore.createQuery(ProcessingJob.class));
        datastore.delete(datastore.createQuery(SourceDocumentReference.class));
        datastore.delete(datastore.createQuery(SourceDocumentReferenceProcessingProfile.class));
    }

    private ProcessingJobTuple createJob (ReferenceOwner owner, String url, URLSourceType sourceType, DocumentReferenceTaskType taskType, boolean isActive, DateTime dateTime) {
        final SourceDocumentReference sourceDocumentReference = new SourceDocumentReference(owner, url,
                                                                                            null, null, null, null, true);

        final List<ProcessingJobSubTask> subTasks = new ArrayList();
        subTasks.addAll(SubTaskBuilder.colourExtraction());
        subTasks.addAll(SubTaskBuilder.thumbnailGeneration());

        final ProcessingJob processingJob = new ProcessingJob(0, new Date(), owner,
                                                              Arrays.asList(new ProcessingJobTaskDocumentReference(taskType,
                                                                                                                   sourceDocumentReference
                                                                                                                           .getId(),
                                                                                                                   subTasks)),
                                                              JobState.READY, sourceType, url, true);

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles = Arrays.asList(new SourceDocumentReferenceProcessingProfile(isActive,
                                                                                                                                                                    owner,
                                                                                                                                                                    sourceDocumentReference.getId(),
                                                                                                                                                                    sourceType,
                                                                                                                                                                    taskType,
                                                                                                                                                                    0,
                                                                                                                                                                    dateTime.toDate(),
                                                                                                                                                                    Days.ONE.toStandardSeconds().getSeconds() * 365
                                                                                                                                                                    ));

        return new ProcessingJobTuple(processingJob, sourceDocumentReference, sourceDocumentReferenceProcessingProfiles);
    }

    private DocumentReferenceTaskType getTaskType (final URLSourceType urlSourceType) {
        switch (urlSourceType) {
            case ISSHOWNAT: return DocumentReferenceTaskType.CHECK_LINK;
            default: return DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD;
        }
    }

    @Test
    public void test_restartJobs() throws MalformedURLException, UnknownHostException, URISyntaxException, ExecutionException {
//        final List<ProcessingJobTuple> processingJobTuples = new ArrayList<>();
//
//        final Random random = new Random(System.nanoTime());
//        final int urlSourceTypeSize = URLSourceType.values().length;
//        final int taskTypeSize = DocumentReferenceTaskType.values().length;
//
//
//        //create valid jobs that have to be updated
//        for (int i = 0; i < 150; ++i) {
//            final String recordId = UUID.randomUUID().toString();
//            final String url = new URI("http", UUID.randomUUID().toString().replace("-", ""), "/test").toString();
//            final URLSourceType urlSourceType = URLSourceType.values()[random.nextInt(urlSourceTypeSize)];
//
//            processingJobTuples.add(createJob(new ReferenceOwner(recordId, recordId, recordId, recordId), url,
//                                              urlSourceType, getTaskType(urlSourceType), true,
//                                              DateTime.now().minusDays(random.nextInt(500) + 1))
//                                   );
//        }
//
//        final List<ProcessingJobTuple> invalidJobsForRestart = new ArrayList<>();
//
//        //create invalid jobs that have to be updated
//        for (int i = 0; i < 150; ++i) {
//            final String recordId = UUID.randomUUID().toString();
//            final String url = new URI("http", UUID.randomUUID().toString().replace("-", ""), "/test").toString();
//
//            boolean isActive = true;
//            DateTime dateTime = DateTime.now();
//            if (random.nextBoolean()) {
//               isActive = false;
//                dateTime = dateTime.minusDays(random.nextInt(500) + 1);
//            }
//            else {
//                dateTime = dateTime.plusDays(random.nextInt(500) + 1);
//            }
//
//            invalidJobsForRestart.add(createJob(new ReferenceOwner(recordId, recordId, recordId, recordId), url,
//                                                URLSourceType.values()[random.nextInt(urlSourceTypeSize)],
//                                                DocumentReferenceTaskType.values()[random.nextInt(taskTypeSize)],
//                                                isActive,
//                                                dateTime
//                                               )
//                                     );
//        }
//
//
//        for (final ProcessingJobTuple jobTuple: processingJobTuples) {
//            processingJobDao.create(jobTuple.getProcessingJob(), WriteConcern.ACKNOWLEDGED);
//            sourceDocumentReferenceDao.create(jobTuple.getSourceDocumentReference(), WriteConcern.ACKNOWLEDGED);
//            sourceDocumentReferenceProcessingProfileDao.createOrModify(jobTuple.getSourceDocumentReferenceProcessingProfiles(), WriteConcern.ACKNOWLEDGED);
//        }
//
//        for (final ProcessingJobTuple jobTuple: invalidJobsForRestart) {
//            processingJobDao.createOrModify(jobTuple.getProcessingJob(), WriteConcern.ACKNOWLEDGED);
//            sourceDocumentReferenceDao.createOrModify(jobTuple.getSourceDocumentReference(), WriteConcern.ACKNOWLEDGED);
//            sourceDocumentReferenceProcessingProfileDao.createOrModify(jobTuple.getSourceDocumentReferenceProcessingProfiles(), WriteConcern.ACKNOWLEDGED);
//        }
//
//        final long timestamp = DateTime.now().plusMonths(12).toDate().getTime();
//        helper.reloadJobs();
//
//        for (final ProcessingJobTuple jobTuple: processingJobTuples) {
//            for (final SourceDocumentReferenceProcessingProfile profile: jobTuple.getSourceDocumentReferenceProcessingProfiles()) {
//                final SourceDocumentReferenceProcessingProfile newProfile = sourceDocumentReferenceProcessingProfileDao.read(profile.getId());
//
//                assertTrue (profile.getActive());
//                assertTrue(newProfile.getActive());
//
//                assertTrue(profile.getToBeEvaluatedAt().before(newProfile.getToBeEvaluatedAt()));
//
//                assertNotNull (newProfile);
//                assertNotNull (newProfile.getToBeEvaluatedAt());
//                assertNotNull (newProfile.getToBeEvaluatedAt().getTime());
//                assertTrue (newProfile.getToBeEvaluatedAt().getTime() >= timestamp);
//            }
//        }
//
//        for (final ProcessingJobTuple jobTuple: invalidJobsForRestart) {
//            for (final SourceDocumentReferenceProcessingProfile profile: jobTuple.getSourceDocumentReferenceProcessingProfiles()) {
//                final SourceDocumentReferenceProcessingProfile newProfile = sourceDocumentReferenceProcessingProfileDao.read(profile.getId());
//
//                ReflectionAssert.assertReflectionEquals(profile, newProfile);
//            }
//        }
    }


}
