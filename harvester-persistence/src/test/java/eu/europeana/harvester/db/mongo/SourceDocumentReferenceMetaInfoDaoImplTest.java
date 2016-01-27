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
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class SourceDocumentReferenceMetaInfoDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(SourceDocumentReferenceMetaInfoDaoImplTest.class.getName());

    private SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    private MongodProcess mongod = null;
    private MongodExecutable mongodExecutable = null;
    private int port = 12345;

    public SourceDocumentReferenceMetaInfoDaoImplTest() throws IOException {

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

        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);
    }

    @After
    public void tearDown() {
        mongodExecutable.stop();
    }


    @Test
    public void test_CreateOrModify_NullCollection() {
        assertFalse (sourceDocumentReferenceMetaInfoDao.createOrModify(null, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void test_CreateOrModify_EmptyCollection() {
        assertFalse(sourceDocumentReferenceMetaInfoDao.createOrModify(Collections.EMPTY_LIST, WriteConcern.NONE)
                                                      .iterator().hasNext());
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);

        assertNotNull(sourceDocumentReferenceMetaInfo.getId());

        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo, WriteConcern.NONE);
        assertEquals(sourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());

        sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId());
    }

    @Test
    public void test_CreateOrModify_ManyElements() {
        final List<SourceDocumentReferenceMetaInfo> metaInfos = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            metaInfos.add(
                new SourceDocumentReferenceMetaInfo(Integer.toString(i), new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null)
            );
        }

        sourceDocumentReferenceMetaInfoDao.createOrModify(metaInfos, WriteConcern.NONE);

        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final SourceDocumentReferenceMetaInfo writtenInfo = sourceDocumentReferenceMetaInfoDao.read(metaInfo.getId());

            sourceDocumentReferenceMetaInfoDao.delete(metaInfo.getId());
            ReflectionAssert.assertReflectionEquals(metaInfo, writtenInfo);
        }
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read("");
        assertNull(sourceDocumentReferenceMetaInfoFromRead);

        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);
        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo, WriteConcern.NONE);
        sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());

        assertEquals(sourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoFromRead.getImageMetaInfo().getHeight());

        sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);
        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo, WriteConcern.NONE);

        final SourceDocumentReferenceMetaInfo updatedSourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo(sourceDocumentReferenceMetaInfo.getId(),
                        new ImageMetaInfo(20, 20, "", "", "", null, null, null), null, null, null);

        assertNotEquals(updatedSourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());
        assertTrue(sourceDocumentReferenceMetaInfoDao.update(updatedSourceDocumentReferenceMetaInfo, WriteConcern.NONE));
        assertEquals(updatedSourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());

        sourceDocumentReferenceMetaInfoDao.delete(updatedSourceDocumentReferenceMetaInfo.getId());

        final SourceDocumentReferenceMetaInfo checkUpdateSourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(20, 20, "", "", "", null, null, null), null, null, null);
        assertFalse(sourceDocumentReferenceMetaInfoDao.update(checkUpdateSourceDocumentReferenceMetaInfo,
                WriteConcern.NONE));
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);
        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo, WriteConcern.NONE);

        SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());
        assertNotNull(sourceDocumentReferenceMetaInfoFromRead);

        assertTrue(sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId()).getN() == 1);

        sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());
        assertNull(sourceDocumentReferenceMetaInfoFromRead);

        assertFalse(sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId()).getN() == 1);
    }

}
