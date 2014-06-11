package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class SourceDocumentReferenceMetaInfoDaoImplTest {

    private SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "europeana";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", ""), null, null);

        assertNotNull(sourceDocumentReferenceMetaInfo.getId());

        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo);
        assertEquals(sourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());

        sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo);
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read("");
        assertNull(sourceDocumentReferenceMetaInfoFromRead);

        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", ""), null, null);
        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo);
        sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());

        assertEquals(sourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoFromRead.getImageMetaInfo().getHeight());

        sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo);
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", ""), null, null);
        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo);

        final SourceDocumentReferenceMetaInfo updatedSourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo(sourceDocumentReferenceMetaInfo.getId(), "a",
                        new ImageMetaInfo(20, 20, "", "", ""), null, null);

        assertNotEquals(updatedSourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());
        assertTrue(sourceDocumentReferenceMetaInfoDao.update(updatedSourceDocumentReferenceMetaInfo));
        assertEquals(updatedSourceDocumentReferenceMetaInfo.getImageMetaInfo().getHeight(),
                sourceDocumentReferenceMetaInfoDao.read(
                        sourceDocumentReferenceMetaInfo.getId()).getImageMetaInfo().getHeight());

        sourceDocumentReferenceMetaInfoDao.delete(updatedSourceDocumentReferenceMetaInfo);

        final SourceDocumentReferenceMetaInfo checkUpdateSourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(20, 20, "", "", ""), null, null);
        assertFalse(sourceDocumentReferenceMetaInfoDao.update(checkUpdateSourceDocumentReferenceMetaInfo));
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo =
                new SourceDocumentReferenceMetaInfo("a", new ImageMetaInfo(10, 10, "", "", ""), null, null);
        sourceDocumentReferenceMetaInfoDao.create(sourceDocumentReferenceMetaInfo);

        SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());
        assertNotNull(sourceDocumentReferenceMetaInfoFromRead);

        assertTrue(sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo));

        sourceDocumentReferenceMetaInfoFromRead =
                sourceDocumentReferenceMetaInfoDao.read(sourceDocumentReferenceMetaInfo.getId());
        assertNull(sourceDocumentReferenceMetaInfoFromRead);

        assertFalse(sourceDocumentReferenceMetaInfoDao.delete(sourceDocumentReferenceMetaInfo));
    }

}
