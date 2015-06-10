package client;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertFalse;

/**
 * Created by salexandru on 10.06.2015.
 */
public class HarvesterClientTest {
    private HarvesterClient harvesterClient;
    private SourceDocumentReferenceDao sourceDocumentReferenceDao;
    private SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;
    private SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;
    private MachineResourceReferenceDao machineResourceReferenceDao;
    private ProcessingJobDao processingJobDao;


    private Datastore datastore;


    @Before
    public void setUp() throws UnknownHostException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        Morphia morphia = new Morphia();
        String dbName = "europeana";

        datastore = morphia.createDatastore(mongo, dbName);

        sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        sourceDocumentProcessingStatisticsDao = new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);
        machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        processingJobDao = new ProcessingJobDaoImpl(datastore);

        harvesterClient = new HarvesterClientImpl(processingJobDao,
                                                  machineResourceReferenceDao,
                                                  sourceDocumentProcessingStatisticsDao,
                                                  sourceDocumentReferenceDao,
                                                  sourceDocumentReferenceMetaInfoDao,
                                                  new HarvesterClientConfig(WriteConcern.NONE)
        );
    }

    @After
    public void tearDown() {
        datastore.delete(datastore.createQuery(ProcessingJob.class));
        datastore.delete(datastore.createQuery(MachineResourceReference.class));
        datastore.delete(datastore.createQuery(SourceDocumentProcessingStatistics.class));
        datastore.delete(datastore.createQuery(SourceDocumentReferenceMetaInfo.class));
        datastore.delete(datastore.createQuery(SourceDocumentReference.class));
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
                URLSourceType.values()[random.nextInt(URLSourceType.values().length)],
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
            final SourceDocumentReference writtenReference = sourceDocumentReferenceDao.read(reference.getId());

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
                                                                             URLSourceType.values()[random.nextInt(URLSourceType.values().length)],
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
                                                                                 URLSourceType.values()[random.nextInt(URLSourceType.values().length)],
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
            final SourceDocumentReference writtenReference = sourceDocumentReferenceDao.read(reference.getId());

            ReflectionAssert.assertReflectionEquals(reference, writtenReference);
        }
    }



}
