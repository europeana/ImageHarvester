package eu.europeana.harvester.db.mongo;

import com.mongodb.MongoClient;
import eu.europeana.harvester.domain.SourceDocumentReference;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class SourceDocumentReferenceDaoImplTest {

    private Datastore datastore;

    @Before
    public void setUp() throws Exception {
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "europeana";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreate() throws Exception {
        SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(1l, 1l, 1l, "test", false, false, false);
        SourceDocumentReferenceDaoImpl sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);

        sourceDocumentReferenceDao.create(sourceDocumentReference);
        assertEquals(sourceDocumentReference.getUrl(),
                sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference);
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReferenceDaoImpl sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        SourceDocumentReference sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read("");

        assertNull(sourceDocumentReferenceFromRead);

        SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(1l, 1l, 1l, "test", false, false, false);
        sourceDocumentReferenceDao.create(sourceDocumentReference);

        sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read(sourceDocumentReference.getId());

        assertEquals(sourceDocumentReference.getUrl(), sourceDocumentReferenceFromRead.getUrl());

        sourceDocumentReferenceDao.delete(sourceDocumentReference);
    }

    @Test
    public void testUpdate() throws Exception {
        SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(1l, 1l, 1l, "test", false, false, false);
        SourceDocumentReferenceDaoImpl sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);

        sourceDocumentReferenceDao.create(sourceDocumentReference);

        SourceDocumentReference newSourceDocumentRefrence =
                new SourceDocumentReference(sourceDocumentReference.getId(), sourceDocumentReference.getProviderId(),
                        sourceDocumentReference.getCollectionId(), sourceDocumentReference.getRecordId(),
                        sourceDocumentReference.getUrl(), true, true, true);

        sourceDocumentReferenceDao.update(newSourceDocumentRefrence);

        assertEquals(sourceDocumentReferenceDao.read(sourceDocumentReference.getId()).getChecked(), true);
        sourceDocumentReferenceDao.delete(newSourceDocumentRefrence);
    }

    @Test
    public void testDelete() throws Exception {
        SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(1l, 1l, 1l, "test", false, false, false);
        SourceDocumentReferenceDaoImpl sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);

        sourceDocumentReferenceDao.create(sourceDocumentReference);

        SourceDocumentReference sourceDocumentReferenceFromRead =
                sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertNotNull(sourceDocumentReferenceFromRead);

        sourceDocumentReferenceDao.delete(sourceDocumentReference);

        sourceDocumentReferenceFromRead = sourceDocumentReferenceDao.read(sourceDocumentReference.getId());
        assertNull(sourceDocumentReferenceFromRead);
    }
}
