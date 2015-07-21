package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.*;

public class SourceDocumentReferenceProcessingProfileDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(SourceDocumentReferenceProcessingProfileDaoImplTest.class.getName());

    private SourceDocumentReferenceProcessingProfileDaoImpl processingProfileDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "harvester_persistency";

//            String username  ;//= "harvester_persistency";
//            String password ;//= "Nhck0zCfcu0M6kK";
//
//            boolean auth = mongo.getDB("admin").authenticate(username, password.toCharArray());
//
//            if (!auth) {
//                fail("couldn't authenticate " + username + " against admin db");
//            }

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        processingProfileDao = new SourceDocumentReferenceProcessingProfileDaoImpl(datastore);
    }

    @After
    public void tearDown() {
        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "harvester_persistency";

            //            String username  ;//= "harvester_persistency";
            //            String password ;//= "Nhck0zCfcu0M6kK";
            //
            //            boolean auth = mongo.getDB("admin").authenticate(username, password.toCharArray());
            //
            //            if (!auth) {
            //                fail("couldn't authenticate " + username + " against admin db");
            //            }

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }
        datastore.delete(datastore.createQuery(SourceDocumentReferenceProcessingProfile.class));
    }

    @Test
    public void test_CreateOrModify_NullCollection() throws Exception {
        assertFalse(processingProfileDao.createOrModify((Collection) null, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void test_CreateOrModify_EmptyCollection() throws Exception {
        assertFalse(processingProfileDao.createOrModify(Collections.EMPTY_LIST, WriteConcern.NONE).iterator().hasNext());
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentReferenceProcessingProfile profile =
                new SourceDocumentReferenceProcessingProfile(true, new ReferenceOwner("1", "1", "1"), "1", URLSourceType.HASVIEW, DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);
        assertNotNull(profile.getId());

        processingProfileDao.createOrModify(profile, WriteConcern.NONE);

        ReflectionAssert.assertReflectionEquals(profile, processingProfileDao.read(profile.getId()));
    }

    @Test
    public void test_CreateOrModify_ManyElements() throws Exception {
        final List<SourceDocumentReferenceProcessingProfile> documentProfiles = new ArrayList<>();

        for (int i = 0; i < 50; ++i) {
            final String iString = Integer.toString(i);
            documentProfiles.add(
                                          new SourceDocumentReferenceProcessingProfile(iString,
                                                                      true, new ReferenceOwner(iString, iString, iString, iString),
                                                                      "1", URLSourceType.HASVIEW, DocumentReferenceTaskType.CHECK_LINK, 0, null, 0
                                          )
                                  );
        }
        processingProfileDao.createOrModify(documentProfiles, WriteConcern.NONE);

        for (final SourceDocumentReferenceProcessingProfile document: documentProfiles) {
            final SourceDocumentReferenceProcessingProfile  writtenDocument = processingProfileDao.read(document.getId());

            ReflectionAssert.assertReflectionEquals(document, writtenDocument);
        }
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentReferenceProcessingProfile profileFromRead = processingProfileDao.read("");
        assertNull(profileFromRead);

        final SourceDocumentReferenceProcessingProfile profile =
                new SourceDocumentReferenceProcessingProfile(true, new ReferenceOwner("1", "1", "1"), "1", URLSourceType.HASVIEW,

                                                             DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);

        processingProfileDao.createOrModify(profile, WriteConcern.NONE);

        profileFromRead = processingProfileDao.read(profile.getId());

        ReflectionAssert.assertReflectionEquals(profile, profileFromRead);
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentReferenceProcessingProfile profile =
                new SourceDocumentReferenceProcessingProfile(true, new ReferenceOwner("1", "1", "1"), "1", URLSourceType.HASVIEW, DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);
        assertFalse(processingProfileDao.update(profile, WriteConcern.NONE));
        processingProfileDao.createOrModify(profile, WriteConcern.NONE);

        final SourceDocumentReferenceProcessingProfile newProfile =
                new SourceDocumentReferenceProcessingProfile(profile.getId(),
                                            false,
                                            profile.getReferenceOwner(), "1", URLSourceType.HASVIEW, DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);
        assertTrue(processingProfileDao.update(newProfile, WriteConcern.ACKNOWLEDGED));

        ReflectionAssert.assertReflectionEquals(newProfile, processingProfileDao.read(profile.getId()));
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentReferenceProcessingProfile profile =
                new SourceDocumentReferenceProcessingProfile(true, new ReferenceOwner("1", "1", "1"), "1", URLSourceType.HASVIEW, DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);
        assertFalse(processingProfileDao.delete(profile.getId()).getN() == 1);
        processingProfileDao.createOrModify(profile, WriteConcern.NONE);

        SourceDocumentReferenceProcessingProfile profileFromRead =
                processingProfileDao.read(profile.getId());
        assertNotNull(profileFromRead);

        assertTrue(processingProfileDao.delete(profile.getId()).getN() == 1);

        profileFromRead = processingProfileDao.read(profile.getId());
        assertNull(profileFromRead);

        assertFalse(processingProfileDao.delete(profile.getId()).getN() == 1);
    }

    @Test
    public void testCreateOrModify() throws Exception {
        final SourceDocumentReferenceProcessingProfile profile =
                new SourceDocumentReferenceProcessingProfile(true, new ReferenceOwner("1", "1", "1"), "1", URLSourceType.HASVIEW,
                                                             DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);
        assertNull(processingProfileDao.read(profile.getId()));

        processingProfileDao.createOrModify(profile, WriteConcern.NONE);
        assertNotNull(processingProfileDao.read(profile.getId()));

        ReflectionAssert.assertReflectionEquals(profile, processingProfileDao.read(profile.getId()));

        final SourceDocumentReferenceProcessingProfile updatedProfile =
                new SourceDocumentReferenceProcessingProfile(profile.getId(), false,
                                            new ReferenceOwner("1", "1", "2"), "1", URLSourceType.HASVIEW, DocumentReferenceTaskType.CHECK_LINK, 0, null, 0);
        processingProfileDao.createOrModify(updatedProfile, WriteConcern.NONE);
        ReflectionAssert.assertReflectionEquals(processingProfileDao.read(profile.getId()), updatedProfile);

    }

    @Test
    public void testGetEvaluatedAt() throws Exception {
        final Map<String, SourceDocumentReferenceProcessingProfile> validProfiles = new HashMap<>();

        //create valid jobs that have to be updated
        final Random random = new Random(System.nanoTime());
        final int urlSourceTypeSize = URLSourceType.values().length;
        final int taskTypeSize = DocumentReferenceTaskType.values().length;
        for (int i = 0; i < 150; ++i) {
            final String recordId = UUID.randomUUID().toString();
            final String url = new URI("http", UUID.randomUUID().toString().replace("-", ""), "/test").toString();

            final SourceDocumentReferenceProcessingProfile profile = new SourceDocumentReferenceProcessingProfile(
                 true,
                 new ReferenceOwner(recordId, recordId, recordId, recordId),
                 url,
                 URLSourceType.values()[random.nextInt(urlSourceTypeSize)],
                 DocumentReferenceTaskType.values()[random.nextInt(taskTypeSize)],
                 0,
                 DateTime.now().minusDays(random.nextInt(500) + 1).toDate(),
                 100
            );

            validProfiles.put(profile.getId(), profile);
        }
        for (int i = 0; i < 350; ++i) {
            final String recordId = UUID.randomUUID().toString();
            final String url = new URI("http", UUID.randomUUID().toString().replace("-", ""), "/test").toString();

            boolean isActive = true;
            DateTime dateTime = DateTime.now();
            if (random.nextBoolean()) {
                isActive = false;
                dateTime = dateTime.minusDays(random.nextInt(500) + 1);
            }
            else {
                dateTime = dateTime.plusDays(random.nextInt(500) + 1);
            }

            final SourceDocumentReferenceProcessingProfile profile = new SourceDocumentReferenceProcessingProfile(
                 isActive,
                 new ReferenceOwner(recordId, recordId, recordId, recordId),
                 url,
                 URLSourceType.values()[random.nextInt(urlSourceTypeSize)],
                 DocumentReferenceTaskType.values()[random.nextInt(taskTypeSize)],
                 0,
                 dateTime.toDate(),
                 100
            );
            processingProfileDao.create(profile, WriteConcern.ACKNOWLEDGED);
        }

        processingProfileDao.createOrModify(validProfiles.values(), WriteConcern.ACKNOWLEDGED);

        assertEquals (validProfiles.size(), processingProfileDao.getJobToBeEvaluated().size());

        for (final SourceDocumentReferenceProcessingProfile profile: processingProfileDao.getJobToBeEvaluated()) {
            final SourceDocumentReferenceProcessingProfile validProfile = validProfiles.get(profile.getId());

            ReflectionAssert.assertReflectionEquals(validProfile, profile);
        }
    }
}