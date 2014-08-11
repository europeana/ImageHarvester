package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class SourceDocumentReferenceDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(SourceDocumentReferenceDaoImplTest.class.getName());

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
            LOG.error(e.getMessage());
        }

        sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertNotNull(sourceDocumentReference.getId());

        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);
        assertEquals(sourceDocumentReference.getUrl(),
                sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference.getId());
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReference sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read("");
        assertNull(sourceDocumentReferenceFromRead);

        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertEquals(sourceDocumentReference.getUrl(), sourceDocumentReferenceFromRead.getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertFalse(sourceDocumentReferenceDao.update(sourceDocumentReference, WriteConcern.NONE));
        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        final SourceDocumentReference newSourceDocumentReference =
                new SourceDocumentReference(sourceDocumentReference.getId(),
                        sourceDocumentReference.getReferenceOwner(), "test2", null, null, 0l, null);
        assertTrue(sourceDocumentReferenceDao.update(newSourceDocumentReference, WriteConcern.NONE));

        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(), "test2");
        sourceDocumentReferenceDao.delete(newSourceDocumentReference.getId());
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertFalse(sourceDocumentReferenceDao.delete(sourceDocumentReference.getId()).getN() == 1);
        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        SourceDocumentReference sourceDocumentReferenceFromRead =
                sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertNotNull(sourceDocumentReferenceFromRead);

        assertTrue(sourceDocumentReferenceDao.delete(sourceDocumentReference.getId()).getN() == 1);

        sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertNull(sourceDocumentReferenceFromRead);

        assertFalse(sourceDocumentReferenceDao.delete(sourceDocumentReference.getId()).getN() == 1);
    }

    @Test
    public void testCreateOrModify() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        assertNull(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()));

        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);
        assertNotNull(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()));
        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(),
                sourceDocumentReference.getUrl());

        final SourceDocumentReference updatedSourceDocumentReference =
                new SourceDocumentReference(sourceDocumentReference.getId(),
                        new ReferenceOwner("1", "1", "1"), "test2", null, null, 0l, null);
        sourceDocumentReferenceDao.createOrModify(updatedSourceDocumentReference, WriteConcern.NONE);
        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(),
                updatedSourceDocumentReference.getUrl());
        assertNotEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl(),
                sourceDocumentReference.getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference.getId());
    }

    @Test
    public void testFindByUrl() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "test", null, null, 0l, null);
        final String url = "test";

        sourceDocumentReferenceDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        sourceDocumentReferenceDao.delete(sourceDocumentReference.getId());
    }

}
