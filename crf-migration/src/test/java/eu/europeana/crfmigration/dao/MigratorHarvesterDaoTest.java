package eu.europeana.crfmigration.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.jobcreator.logic.ProcessingJobBuilder;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.MigratorUtils;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;
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

    @Before
    public void setUp() throws UnknownHostException {
        migratorConfig = MigratorUtils.createMigratorConfig("config-files/migration.conf");
        final MorphiaDataStore morphiaDataStore =  new MorphiaDataStore(migratorConfig.getTargetMongoConfig().getMongoServerAddressList(),
                                                                          migratorConfig.getTargetMongoConfig().getDbName()
                                                                         );

        if (StringUtils.isNotEmpty(migratorConfig.getTargetMongoConfig().getUsername())) {
            morphiaDataStore.getMongo().getDB("admin").authenticate(migratorConfig.getTargetMongoConfig().getUsername(),
                                                                    migratorConfig.getTargetMongoConfig().getPassword().toCharArray());
        }
        dataStore = morphiaDataStore.getDatastore();

        harvesterDao = new MigratorHarvesterDao(migratorConfig.getTargetMongoConfig());
    }

    @After
    public void tearDown() {
        dataStore.delete(dataStore.createQuery(SourceDocumentReference.class));
        dataStore.delete(dataStore.createQuery(ProcessingJob.class));
        dataStore.delete(dataStore.createQuery(SourceDocumentReferenceProcessingProfile.class));
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
        final ProcessingJobTuple job = ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com",
                                                                                              owner, JobPriority.NORMAL.getPriority(), falseOption).get(0);
        harvesterDao.saveProcessingJobTuples(Arrays.asList(job), migrationBatchId);
        final Query<ProcessingJob> query = dataStore.createQuery(ProcessingJob.class);
        query.filter("_id", job.getProcessingJob().getId());
        assertEquals(1, dataStore.getCount(query));

        final Query<SourceDocumentReference> sourceDocumentReferenceQuery = dataStore.createQuery(SourceDocumentReference.class);
        sourceDocumentReferenceQuery.filter("_id", job.getSourceDocumentReference().getId());
        assertEquals(1, dataStore.getCount(sourceDocumentReferenceQuery));

        for (final SourceDocumentReferenceProcessingProfile profile: job.getSourceDocumentReferenceProcessingProfiles()) {
            final Query<SourceDocumentReferenceProcessingProfile> profileQuery = dataStore.createQuery(SourceDocumentReferenceProcessingProfile.class);
            profileQuery.filter("_id", profile.getId());
            assertEquals(1, dataStore.getCount(profileQuery));
        }
    }

    @Test
    public void test_ProcessJobs_ManyElements() throws MalformedURLException, UnknownHostException,
                                                       InterruptedException, ExecutionException, TimeoutException {
        final List<ProcessingJobTuple> processingJobList = new ArrayList<>();

        processingJobList.add(ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com", owner,JobPriority.NORMAL.getPriority(), falseOption).get(0));
        processingJobList.add(ProcessingJobBuilder.edmIsShownByUrlJobs("http://www.skype.com", owner,JobPriority.NORMAL.getPriority(), falseOption).get(0));
        processingJobList.add(ProcessingJobBuilder.edmIsShownAtUrlJobs("http://www.yahoo.com", owner,JobPriority.NORMAL.getPriority(), falseOption).get(0));

        harvesterDao.saveProcessingJobTuples(processingJobList, migrationBatchId);
        assertEquals(3, dataStore.getCount(ProcessingJob.class));

        for (final ProcessingJobTuple job: processingJobList) {
            final Query<ProcessingJob> query = dataStore.createQuery(ProcessingJob.class);
            query.filter("_id", job.getProcessingJob().getId());
            assertEquals(1, dataStore.getCount(query));

            final Query<SourceDocumentReference> sourceDocumentReferenceQuery = dataStore.createQuery(SourceDocumentReference.class);
            sourceDocumentReferenceQuery.filter("_id", job.getSourceDocumentReference().getId());
            assertEquals(1, dataStore.getCount(sourceDocumentReferenceQuery));

            for (final SourceDocumentReferenceProcessingProfile profile: job.getSourceDocumentReferenceProcessingProfiles()) {
                final Query<SourceDocumentReferenceProcessingProfile> profileQuery = dataStore.createQuery(SourceDocumentReferenceProcessingProfile.class);
                profileQuery.filter("_id", profile.getId());
                assertEquals(1, dataStore.getCount(profileQuery));
            }
        }
    }

}
