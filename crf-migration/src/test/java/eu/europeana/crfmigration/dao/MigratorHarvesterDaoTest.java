package eu.europeana.crfmigration.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.google.common.collect.Lists;
import com.mongodb.ServerAddress;
import eu.europeana.crfmigration.domain.GraphiteReporterConfig;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.jobcreator.logic.ProcessingJobBuilder;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.MongoDBUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MigratorHarvesterDaoTest {



    private MigratorConfig migratorConfig;

    private MigratorHarvesterDao harvesterDao;
    private Datastore dataStore;

    private static final ProcessingJobCreationOptions falseOption = new ProcessingJobCreationOptions(false);
    private static final ReferenceOwner owner = new ReferenceOwner();
    private final static String migrationBatchId = "migration-test-batch";
    private MongoDBUtils mongoDBUtils;


    @Before
    public void setUp() throws IOException, ParseException {
        mongoDBUtils = new MongoDBUtils(migratorConfig);

        final List<ServerAddress> servers = new ArrayList<ServerAddress>();
        servers.add(new ServerAddress("127.0.0.1",27017));
        servers.add(new ServerAddress("127.0.0.1",27017));

        migratorConfig = new MigratorConfig(
                new MongoConfig(servers, "source_migration", "", ""),
                new MongoConfig(servers, "dest_migration", "", ""),
                new GraphiteReporterConfig("127.0.0.1", "test", 10000),
                2,
                new DateTime(
                        2015,
                        12,
                        30,
                        0,
                        10));

        final MorphiaDataStore morphiaDataStore =  new MorphiaDataStore(migratorConfig.getTargetMongoConfig().getMongoServerAddressList(),
                migratorConfig.getTargetMongoConfig().getDbName()
        );
        dataStore = morphiaDataStore.getDatastore();

        harvesterDao = new MigratorHarvesterDao(migratorConfig.getTargetMongoConfig());
    }

    @After
    public void tearDown() throws UnknownHostException, ParseException {
        mongoDBUtils.cleanMongoDatabase();
    }

    @Test
    public void test_ProcessJobs_EmptyList() throws InterruptedException, MalformedURLException, TimeoutException,
            ExecutionException, UnknownHostException {
        harvesterDao.saveProcessingJobTuples(Collections.EMPTY_LIST, migrationBatchId);
        assertEquals(0, dataStore.getCount(ProcessingJob.class));
    }

    @Test
    public void test_ProcessJobs_OneElement() throws MalformedURLException, UnknownHostException, InterruptedException,
            ExecutionException, TimeoutException {
        final List<ProcessingJobTuple> jobs = ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com",
                owner,
                JobPriority.NORMAL.getPriority(),
                falseOption);

        assertEquals(1, jobs.size());

        final ProcessingJobTuple job = jobs.get(0);

        harvesterDao.saveProcessingJobTuples(jobs, migrationBatchId);
        final Query<ProcessingJob> query = dataStore.createQuery(ProcessingJob.class);
        query.filter("_id", job.getProcessingJob().getId());
        assertEquals(0, dataStore.getCount(query));

        final Query<SourceDocumentReference> sourceDocumentReferenceQuery = dataStore.createQuery(SourceDocumentReference.class);
        sourceDocumentReferenceQuery.filter("_id", job.getSourceDocumentReference().getId());
        assertEquals(1, dataStore.getCount(sourceDocumentReferenceQuery));

        /* TODO : Enable again when migration is finished
        for (final SourceDocumentReferenceProcessingProfile profile: job.getSourceDocumentReferenceProcessingProfiles()) {
            final Query<SourceDocumentReferenceProcessingProfile> profileQuery = dataStore.createQuery(SourceDocumentReferenceProcessingProfile.class);
            profileQuery.filter("_id", profile.getId());
            assertEquals(1, dataStore.getCount(profileQuery));
        }
        */
    }

//    @Test
//    public void test_ProcessJobs_ManyElements() throws MalformedURLException, UnknownHostException,
//                                                       InterruptedException, ExecutionException, TimeoutException {
//        final List<ProcessingJobTuple> processingJobList = new ArrayList<>();
//
//        processingJobList.add(ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com", owner,JobPriority.NORMAL.getPriority(), falseOption).get(0));
//        processingJobList.add(ProcessingJobBuilder.edmIsShownByUrlJobs("http://www.skype.com", owner,JobPriority.NORMAL.getPriority(), falseOption).get(0));
//        processingJobList.add(ProcessingJobBuilder.edmIsShownAtUrlJobs("http://www.yahoo.com", owner,JobPriority.NORMAL.getPriority(), falseOption).get(0));
//
//        harvesterDao.saveProcessingJobTuples(processingJobList, migrationBatchId);
//         assertEquals(3, dataStore.getCount(ProcessingJob.class));
//
//        for (final ProcessingJobTuple job: processingJobList) {
//            final Query<ProcessingJob> query = dataStore.createQuery(ProcessingJob.class);
//            query.filter("_id", job.getProcessingJob().getId());
//            assertEquals(1, dataStore.getCount(query));
//
//            final Query<SourceDocumentReference> sourceDocumentReferenceQuery = dataStore.createQuery(SourceDocumentReference.class);
//            sourceDocumentReferenceQuery.filter("_id", job.getSourceDocumentReference().getId());
//            assertEquals(1, dataStore.getCount(sourceDocumentReferenceQuery));
//
//            /*
//                TODO : Enable again when migration is finished
//            for (final SourceDocumentReferenceProcessingProfile profile: job.getSourceDocumentReferenceProcessingProfiles()) {
//                final Query<SourceDocumentReferenceProcessingProfile> profileQuery = dataStore.createQuery(SourceDocumentReferenceProcessingProfile.class);
//                profileQuery.filter("_id", profile.getId());
//                assertEquals(1, dataStore.getCount(profileQuery));
//            }
//            */
//        }
//    }

}
