package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.common.collect.Lists;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class WebResourceMetaInfoDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(WebResourceMetaInfoDaoImplTest.class.getName());

    private WebResourceMetaInfoDao webResourceMetaInfoDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;
        MongoClient mongo = null;
        String dbName = "harvester_persistency";
        Morphia morphia = null;

        try {
            mongo = new MongoClient("localhost", 27017);
            morphia = new Morphia();

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

        webResourceMetaInfoDao = new WebResourceMetaInfoDaoImpl(mongo.getDB(dbName),morphia,datastore);
    }

    @Test
    public void test_CreateOrModify_NullCollection() {
         assertTrue(webResourceMetaInfoDao.createOrModify(null, WriteConcern.NONE) == 0);
    }

    @Test
    public void test_CreateOrModify_EmptyCollection() {
        assertTrue(webResourceMetaInfoDao.createOrModify(Collections.EMPTY_LIST, WriteConcern.NONE)
                 == 0);
    }

    @Test
    public void testCreate() throws Exception {
        final WebResourceMetaInfo sourceDocumentReferenceMetaInfo =
                new WebResourceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);

        assertNotNull(sourceDocumentReferenceMetaInfo.getId());

        webResourceMetaInfoDao.create(sourceDocumentReferenceMetaInfo, WriteConcern.NONE);
        assertEquals(sourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                webResourceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());

        webResourceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId());
    }

    @Test
    public void test_CreateOrModify_ManyElements() {
        final List<WebResourceMetaInfo> metaInfos = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            metaInfos.add(
                new WebResourceMetaInfo(Integer.toString(i), new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null)
            );
        }

        webResourceMetaInfoDao.createOrModify(metaInfos, WriteConcern.NONE);

        for (final WebResourceMetaInfo metaInfo: metaInfos) {
            final WebResourceMetaInfo writtenInfo = webResourceMetaInfoDao.read(metaInfo.getId());

            webResourceMetaInfoDao.delete(metaInfo.getId());
            ReflectionAssert.assertReflectionEquals(metaInfo, writtenInfo);
        }
    }

    @Test
    public void testRead() throws Exception {
        WebResourceMetaInfo sourceDocumentReferenceMetaInfoFromRead =
                webResourceMetaInfoDao.read("");
        assertNull(sourceDocumentReferenceMetaInfoFromRead);

        final WebResourceMetaInfo sourceDocumentReferenceMetaInfo =
                new WebResourceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);
        webResourceMetaInfoDao.createOrModify(Lists.newArrayList(sourceDocumentReferenceMetaInfo), WriteConcern.ACKNOWLEDGED);

        sourceDocumentReferenceMetaInfoFromRead =
                webResourceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());

        assertEquals(sourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoFromRead.getImageMetaInfo().getHeight());

        webResourceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final WebResourceMetaInfo sourceDocumentReferenceMetaInfo =
                new WebResourceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);
        webResourceMetaInfoDao.createOrModify(Lists.newArrayList(sourceDocumentReferenceMetaInfo), WriteConcern.NONE);

        final WebResourceMetaInfo updatedSourceDocumentReferenceMetaInfo =
                new WebResourceMetaInfo(sourceDocumentReferenceMetaInfo.getId(),
                        new ImageMetaInfo(20, 20, "", "", "", null, null, null), null, null, null);

        assertNotEquals(updatedSourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                webResourceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());
        assertTrue(webResourceMetaInfoDao.createOrModify(Lists.newArrayList(updatedSourceDocumentReferenceMetaInfo), WriteConcern.NONE) == 1);
        assertEquals(updatedSourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                webResourceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());

        webResourceMetaInfoDao.delete(updatedSourceDocumentReferenceMetaInfo.getId());

        final WebResourceMetaInfo checkUpdateSourceDocumentReferenceMetaInfo =
                new WebResourceMetaInfo("a", new ImageMetaInfo(20, 20, "", "", "", null, null, null), null, null, null);
        assertFalse(webResourceMetaInfoDao.createOrModify(Lists.newArrayList(checkUpdateSourceDocumentReferenceMetaInfo),
                WriteConcern.NONE) == 1);
    }

    @Test
    public void testDelete() throws Exception {
        final WebResourceMetaInfo sourceDocumentReferenceMetaInfo =
                new WebResourceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", "", null, null, null), null, null, null);
        webResourceMetaInfoDao.createOrModify(Lists.newArrayList(sourceDocumentReferenceMetaInfo), WriteConcern.NONE);

        WebResourceMetaInfo sourceDocumentReferenceMetaInfoFromRead =
                webResourceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());
        assertNotNull(sourceDocumentReferenceMetaInfoFromRead);

        assertTrue(webResourceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId()).getN() == 1);

        sourceDocumentReferenceMetaInfoFromRead =
                webResourceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());
        assertNull(sourceDocumentReferenceMetaInfoFromRead);

        assertFalse(webResourceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo.getId()).getN() == 1);
    }

}
