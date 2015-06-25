package eu.europeana.crfmigration.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.URLSourceType;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
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
    }

    @Test
    public void test_SourceDocumentReferences_EmptyList() throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        harvesterDao.saveSourceDocumentReferences(Collections.EMPTY_LIST,migrationBatchId);
        assertEquals (0, dataStore.getCount(SourceDocumentReference.class));
    }

    @Test
    public void test_SourceDocumentReferences_OneElement() throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(owner, URLSourceType.ISSHOWNBY, "http://www.google.com",
                                            null, null, null, null, true);
        harvesterDao.saveSourceDocumentReferences(Arrays.asList(sourceDocumentReference),migrationBatchId);
        assertEquals (1, dataStore.getCount(sourceDocumentReference));
    }

    @Test
    public void test_SourceDocumentReferences_ManyElements() throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        final List<SourceDocumentReference> sourceDocumentReferences =new ArrayList<>();
        sourceDocumentReferences.add(
           new SourceDocumentReference(owner, URLSourceType.ISSHOWNBY, "http://www.google.com", null, null, null, null, true)
        );

        sourceDocumentReferences.add(
           new SourceDocumentReference(owner, URLSourceType.ISSHOWNAT, "http://www.skype.com", null, null, null, null, true)
        );

        sourceDocumentReferences.add(
           new SourceDocumentReference(owner, URLSourceType.HASVIEW, "http://www.yahoo.com", null, null, null, null, true)
        );

        sourceDocumentReferences.add(new SourceDocumentReference(owner, null, "http://www.facebook.com", null, null,
                                                                 null, null, true));

        harvesterDao.saveSourceDocumentReferences(sourceDocumentReferences,migrationBatchId);
        assertEquals(4, dataStore.getCount(SourceDocumentReference.class));

        for (final SourceDocumentReference reference: sourceDocumentReferences) {
            final Query<SourceDocumentReference> query = dataStore.createQuery(SourceDocumentReference.class);
            query.filter("_id", reference.getId());
            assertEquals (1, dataStore.getCount(query));
        }
    }

    @Test
    public void test_ProcessJobs_EmptyList() {
        harvesterDao.saveProcessingJobs(Collections.EMPTY_LIST,migrationBatchId);
        assertEquals(0, dataStore.getCount(ProcessingJob.class));
    }

    @Test
    public void test_ProcessJobs_OneElement() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJob> processingJobList = Arrays.asList(ProcessingJobBuilder
                                                                            .edmObjectUrlJobs("http://www.google.com",
                                                                                              owner, falseOption).get(0)
                                                                            .getProcessingJob());
        harvesterDao.saveProcessingJobs(processingJobList,migrationBatchId);
        assertEquals (1, dataStore.getCount(processingJobList.get(0)));
    }

    @Test
    public void test_ProcessJobs_ManyElements() throws MalformedURLException, UnknownHostException {
        final List<ProcessingJob> processingJobList = new ArrayList<>();

        processingJobList.add(ProcessingJobBuilder.edmObjectUrlJobs("http://www.google.com", owner, falseOption).get(0)
                                                 .getProcessingJob());
        processingJobList.add(ProcessingJobBuilder.edmIsShownByUrlJobs("http://www.skype.com", owner, falseOption).get(0)
                                                 .getProcessingJob());
        processingJobList.add(ProcessingJobBuilder.edmIsShownAtUrlJobs("http://www.yahoo.com", owner, falseOption).get(0)
                                                 .getProcessingJob());

        harvesterDao.saveProcessingJobs(processingJobList,migrationBatchId);
        assertEquals(3, dataStore.getCount(ProcessingJob.class));

        for (final ProcessingJob job: processingJobList) {
            final Query<ProcessingJob> query = dataStore.createQuery(ProcessingJob.class);
            query.filter("_id", job.getId());
            assertEquals (1, dataStore.getCount(query));
        }
    }

}
