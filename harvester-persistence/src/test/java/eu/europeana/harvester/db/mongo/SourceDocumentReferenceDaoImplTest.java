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
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class SourceDocumentReferenceDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(SourceDocumentReferenceDaoImplTest.class.getName());

    private SourceDocumentReferenceDaoImpl processingProfileDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "harvester_persistency";

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

        processingProfileDao = new SourceDocumentReferenceDaoImpl(datastore);
    }

    @Test
    public void test_CreateOrModify_NullCollection() throws Exception {
        assertFalse (processingProfileDao.createOrModify((Collection)null, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void test_CreateOrModify_EmptyCollection() throws Exception {
        assertFalse (processingProfileDao.createOrModify(Collections.EMPTY_LIST, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "test", null, null, 0l, null, true);
        assertNotNull(sourceDocumentReference.getId());

        processingProfileDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);
        assertEquals(sourceDocumentReference.getUrl(),
                processingProfileDao.read(sourceDocumentReference.getId()).getUrl());

        processingProfileDao.delete(sourceDocumentReference.getId());
    }

    @Test
    public void test_CreateOrModify_ManyElements() throws Exception {
        final List<SourceDocumentReference> documentReferences = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            final String iString = Integer.toString(i);
            documentReferences.add(
               new SourceDocumentReference(iString, new ReferenceOwner(iString, iString, iString, iString),
                                           null, "test", null, null, 0l, null, true
                                          )
            );
        }
        processingProfileDao.createOrModify(documentReferences, WriteConcern.NONE);

        for (final SourceDocumentReference document: documentReferences) {
            final SourceDocumentReference  writtenDocument = processingProfileDao.read(document.getId());

            processingProfileDao.delete(document.getId());
            ReflectionAssert.assertReflectionEquals(document, writtenDocument);
        }
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReference sourceDocumentReferenceFromRead = processingProfileDao.read("");
        assertNull(sourceDocumentReferenceFromRead);

        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "test", null, null, 0l, null, true);
        processingProfileDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        sourceDocumentReferenceFromRead = processingProfileDao.read(sourceDocumentReference.getId());
        assertEquals(sourceDocumentReference.getUrl(), sourceDocumentReferenceFromRead.getUrl());

        processingProfileDao.delete(sourceDocumentReference.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "test", null, null, 0l, null, true);
        assertFalse(processingProfileDao.update(sourceDocumentReference, WriteConcern.NONE));
        processingProfileDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        final SourceDocumentReference newSourceDocumentReference =
                new SourceDocumentReference(sourceDocumentReference.getId(),
                        sourceDocumentReference.getReferenceOwner(), null, "test2", null, null, 0l, null, true);
        assertTrue(processingProfileDao.update(newSourceDocumentReference, WriteConcern.NONE));

        assertEquals(processingProfileDao.read(sourceDocumentReference.getId()).getUrl(), "test2");
        processingProfileDao.delete(newSourceDocumentReference.getId());
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "test", null, null, 0l, null, true);
        assertFalse(processingProfileDao.delete(sourceDocumentReference.getId()).getN() == 1);
        processingProfileDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        SourceDocumentReference sourceDocumentReferenceFromRead =
                processingProfileDao.read(sourceDocumentReference.getId());
        assertNotNull(sourceDocumentReferenceFromRead);

        assertTrue(processingProfileDao.delete(sourceDocumentReference.getId()).getN() == 1);

        sourceDocumentReferenceFromRead = processingProfileDao.read(sourceDocumentReference.getId());
        assertNull(sourceDocumentReferenceFromRead);

        assertFalse(processingProfileDao.delete(sourceDocumentReference.getId()).getN() == 1);
    }

    @Test
    public void testCreateOrModify() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "test", null, null, 0l, null, true);
        assertNull(processingProfileDao.read(sourceDocumentReference.getId()));

        processingProfileDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);
        assertNotNull(processingProfileDao.read(sourceDocumentReference.getId()));
        assertEquals(processingProfileDao.read(sourceDocumentReference.getId()).getUrl(),
                sourceDocumentReference.getUrl());

        final SourceDocumentReference updatedSourceDocumentReference =
                new SourceDocumentReference(sourceDocumentReference.getId(),
                        new ReferenceOwner("1", "1", "1"), null, "test2", null, null, 0l, null, true);
        processingProfileDao.createOrModify(updatedSourceDocumentReference, WriteConcern.NONE);
        assertEquals(processingProfileDao.read(sourceDocumentReference.getId()).getUrl(),
                updatedSourceDocumentReference.getUrl());
        assertNotEquals(processingProfileDao.read(sourceDocumentReference.getId()).getUrl(),
                sourceDocumentReference.getUrl());

        processingProfileDao.delete(sourceDocumentReference.getId());
    }

    @Test
    public void testFindByUrl() throws Exception {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, "test", null, null, 0l, null, true);
        final String url = "test";

        processingProfileDao.createOrModify(sourceDocumentReference, WriteConcern.NONE);

        processingProfileDao.delete(sourceDocumentReference.getId());
    }
}
