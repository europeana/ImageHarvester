package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReference;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class SourceDocumentReferenceDaoImplTest {

    private SourceDocumentReferenceDaoImpl sourceDocumentReferenceDao;

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

        sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertNotNull(sourceDocumentReference.getId());

        sourceDocumentReferenceDao.create(sourceDocumentReference);
        assertEquals(sourceDocumentReference.getUrl(),
                sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference);
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReference sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read("");
        assertNull(sourceDocumentReferenceFromRead);

        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        sourceDocumentReferenceDao.create(sourceDocumentReference);

        sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertEquals(sourceDocumentReference.getUrl(), sourceDocumentReferenceFromRead.getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference);
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertFalse(sourceDocumentReferenceDao.update(sourceDocumentReference));
        sourceDocumentReferenceDao.create(sourceDocumentReference);

        final SourceDocumentReference newSourceDocumentReference =
                new SourceDocumentReference(sourceDocumentReference.getId(),
                        sourceDocumentReference.getReferenceOwner(), "test2", null, null, 0l, null);
        assertTrue(sourceDocumentReferenceDao.update(newSourceDocumentReference));

        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(), "test2");
        sourceDocumentReferenceDao.delete(newSourceDocumentReference);
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertFalse(sourceDocumentReferenceDao.delete(sourceDocumentReference));
        sourceDocumentReferenceDao.create(sourceDocumentReference);

        SourceDocumentReference sourceDocumentReferenceFromRead =
                sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertNotNull(sourceDocumentReferenceFromRead);

        assertTrue(sourceDocumentReferenceDao.delete(sourceDocumentReference));

        sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertNull(sourceDocumentReferenceFromRead);

        assertFalse(sourceDocumentReferenceDao.delete(sourceDocumentReference));
    }

    @Test
    public void testCreateOrModify() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertNull(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()));

        sourceDocumentReferenceDao.create(sourceDocumentReference);
        assertNotNull(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()));
        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(),
                sourceDocumentReference.getUrl());

        final SourceDocumentReference updatedSourceDocumentReference =
                new SourceDocumentReference(sourceDocumentReference.getId(),
                        new ReferenceOwner("1", "1", "1"), "test2", null, null, 0l, null);
        sourceDocumentReferenceDao.createOrModify(updatedSourceDocumentReference);
        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(),
                updatedSourceDocumentReference.getUrl());
        assertNotEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(),
                sourceDocumentReference.getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference);
    }

    @Test
    public void testFindByUrl() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        final String url = "test";

        sourceDocumentReferenceDao.create(sourceDocumentReference);

        sourceDocumentReferenceDao.delete(sourceDocumentReference);
    }

}
