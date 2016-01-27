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
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.*;

public class SourceDocumentProcessingStatisticsDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(SourceDocumentProcessingStatisticsDaoImplTest.class.getName());

    private SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    private MongodProcess mongod = null;
    private MongodExecutable mongodExecutable = null;
    private int port = 12345;

    public SourceDocumentProcessingStatisticsDaoImplTest() throws IOException {

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

        try {
            MongoClient mongo = new MongoClient("localhost", port);
            Morphia morphia = new Morphia();
            String dbName = "harvester_persistency";
            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        sourceDocumentProcessingStatisticsDao = new SourceDocumentProcessingStatisticsDaoImpl(datastore);
    }

    @After
    public void tearDown() {
        mongodExecutable.stop();
    }

    @Test
    public void test_CreateOrModify_NullCollection() {
        assertFalse (sourceDocumentProcessingStatisticsDao.createOrModify((Collection)null, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void test_CreateOrModify_EmptyCollection() {
        assertFalse (sourceDocumentProcessingStatisticsDao.createOrModify(Collections.EMPTY_LIST, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), true, null, null, new ReferenceOwner("1", "1", "1"),
                        null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
        assertNotNull(sourceDocumentProcessingStatistics.getId());

        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);
        assertEquals(sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                sourceDocumentProcessingStatisticsDao.read(
                        sourceDocumentProcessingStatistics.getId()).getHttpResponseContentSizeInBytes());

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics.getId());
    }

    @Test
    public void test_CreateOrModify_ManyElements() {
        final List<SourceDocumentProcessingStatistics> documents = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            final String iString = Integer.toString(i);
            documents.add(
               new SourceDocumentProcessingStatistics(iString,
                                                       new Date(),
                                                      new Date(),
                                                      true, null, null,
                                                      new ReferenceOwner(iString, iString, iString, iString),
                                                      null, "", "", 100, "", (i + 1L)*1024l, i * 1L , 0l, 0l, "", null, "",null
                                                    )
            );
        }
        sourceDocumentProcessingStatisticsDao.createOrModify(documents, WriteConcern.NONE);

        for (final SourceDocumentProcessingStatistics document: documents) {
            final SourceDocumentProcessingStatistics writtenDocument = sourceDocumentProcessingStatisticsDao.read(document.getId());

            assertNotNull(writtenDocument);


            ReflectionAssert.assertReflectionEquals(document, writtenDocument);
            sourceDocumentProcessingStatisticsDao.delete(writtenDocument.getId());
        }
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read("");
        assertNull(sourceDocumentProcessingStatisticsFromRead);

        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), true, null, null, new ReferenceOwner("1", "1", "1"),
                        null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);
        sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());

        assertEquals(sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                sourceDocumentProcessingStatisticsFromRead.getHttpResponseContentSizeInBytes());

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), true, null, ProcessingState.DOWNLOADING,
                        new ReferenceOwner("1", "1", "1"), null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
        assertFalse(sourceDocumentProcessingStatisticsDao.update(sourceDocumentProcessingStatistics, WriteConcern.NONE));
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);

        final SourceDocumentProcessingStatistics updatedSourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(sourceDocumentProcessingStatistics.getId(),
                        sourceDocumentProcessingStatistics.getCreatedAt(),
                        sourceDocumentProcessingStatistics.getUpdatedAt(),
                        true, null, ProcessingState.SUCCESS,
                        sourceDocumentProcessingStatistics.getReferenceOwner(),
                        null, sourceDocumentProcessingStatistics.getSourceDocumentReferenceId(),
                        sourceDocumentProcessingStatistics.getProcessingJobId(),
                        sourceDocumentProcessingStatistics.getHttpResponseCode(),
                        sourceDocumentProcessingStatistics.getHttpResponseContentType(),
                        sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                        sourceDocumentProcessingStatistics.getRetrievalDurationInMilliSecs(),
                        sourceDocumentProcessingStatistics.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        sourceDocumentProcessingStatistics.getCheckingDurationInMilliSecs(),
                        sourceDocumentProcessingStatistics.getSourceIp(),
                        sourceDocumentProcessingStatistics.getHttpResponseHeaders(), "",null);
        assertTrue(sourceDocumentProcessingStatisticsDao.update(updatedSourceDocumentProcessingStatistics, WriteConcern.NONE));

        assertEquals(sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId()).getState(),
                ProcessingState.SUCCESS);

        sourceDocumentProcessingStatisticsDao.delete(updatedSourceDocumentProcessingStatistics.getId());
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), true, null, null, new ReferenceOwner("1", "1", "1"),
                        null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
        assertFalse(sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics.getId()).getN() == 1);
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);

        SourceDocumentProcessingStatistics sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());
        assertNotNull(sourceDocumentProcessingStatisticsFromRead);

        assertTrue(sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics.getId()).getN() == 1);

        sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());
        assertNull(sourceDocumentProcessingStatisticsFromRead);
    }

    @Test
    public void testAggregateCount() throws Exception {
        final List<String> ids = new ArrayList<>();
       for (int i = 0; i < 50; ++i) {
           final String id = UUID.randomUUID().toString();
           ids.add(id);
           final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                   new SourceDocumentProcessingStatistics(id, new Date(), new Date(), true, null, ProcessingState.READY, new ReferenceOwner("1", "1", "1"),
                                                          null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
           sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);
       }

        for (int i = 0; i < 100; ++i) {
            final String id = UUID.randomUUID().toString();
            ids.add(id);
            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    new SourceDocumentProcessingStatistics(id,new Date(), new Date(), true, null, ProcessingState.ERROR, new ReferenceOwner("1", "1", "1"),
                                                           null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
            sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);
        }

        for (int i = 0; i < 10; ++i) {
            final String id = UUID.randomUUID().toString();
            ids.add(id);
            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                    new SourceDocumentProcessingStatistics(id,new Date(), new Date(), true, null, ProcessingState.SUCCESS, new ReferenceOwner("1", "1", "1"),
                                                           null, "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null, "",null);
            sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics, WriteConcern.NONE);
        }

        final Map<ProcessingState, Long> counts = sourceDocumentProcessingStatisticsDao.countNumberOfDocumentsWithState();

        for (final String id: ids) {
            sourceDocumentProcessingStatisticsDao.delete(id);
        }

        assertEquals (3, counts.size());
        assertEquals (50L, counts.get(ProcessingState.READY).longValue());
        assertEquals (100L, counts.get(ProcessingState.ERROR).longValue());
        assertEquals (10L, counts.get(ProcessingState.SUCCESS).longValue());
    }

}
