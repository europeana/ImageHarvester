package eu.europeana.crfmigration.dao;

import com.mongodb.*;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.crfmigration.domain.MigratorConfig;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.MongoDBUtils;

import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.*;
import static utils.MigratorUtils.PATH_PREFIX;
import static utils.MigratorUtils.createMigratorConfig;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MigratorEuropeanaDaoTest {
    private MigratorConfig migratorConfig = null;


    private final static Date dateFilter = DateTime.parse("2014-08-15T00:00:00.000Z").toDate();
    private final static String migrationBatchId = "migration-test-batch";
    private MongoDBUtils mongoDBUtils;
    private MigratorEuropeanaDao europeanaDao;

    @Before
    public void setUp() throws UnknownHostException {
        migratorConfig = createMigratorConfig("config-files/migration.conf");
        mongoDBUtils = new MongoDBUtils(migratorConfig);
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/aggregation.json", "Aggregation");
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/record.json", "record");
        europeanaDao = new MigratorEuropeanaDao(migratorConfig.getSourceMongoConfig());
    }

    @After
    public void tearDown() {
       mongoDBUtils.cleanMongoDatabase();
    }

    @Test
    public void test_GenerateCursor_NoFiltering() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null,migrationBatchId);
        assertEquals(new BasicDBObject(), cursor.getQuery());
    }

    @Test
    public void test_GenerateCursor_DateFiltering() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter,migrationBatchId);
        assertEquals (new BasicDBObject("timestampUpdated", new BasicDBObject("$gt", dateFilter)), cursor.getQuery());
    }

    @Test
    public void test_RetrieveData_NoFiltering_BatchSizeAllDB() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null,migrationBatchId);
        final DBCollection mongo = mongoDBUtils.connectToSource().getCollection("record");
        final Map<String, String> records = europeanaDao.retrieveRecordsIdsFromCursor(cursor, mongo.find().size(),migrationBatchId);

        assertEquals (mongo.find().size(), records.size());
        for (final Map.Entry<String, String> record: records.entrySet()) {
            final BasicDBObject query = new BasicDBObject();
            query.put("about", record.getKey());
            query.put("europeanaCollectionName", record.getValue());
            assertEquals(1, mongo.find(query).size());
        }
    }

    @Test
    public void test_RetrieveData_DateFiltering() {
        final BasicDBObject query = new BasicDBObject("timestampUpdated", new BasicDBObject("$gt", dateFilter));
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter,migrationBatchId);
        final DBCollection mongo = mongoDBUtils.connectToSource().getCollection("record");
        final Map<String, String> records = europeanaDao.retrieveRecordsIdsFromCursor(cursor, mongo.find(query).size(),migrationBatchId);

        assertEquals(mongo.find(query).size(), records.size());
        for (final Map.Entry<String, String> record: records.entrySet()) {
            final BasicDBObject filteringQuery = new BasicDBObject(query);
            filteringQuery.put("about", record.getKey());
            filteringQuery.put("europeanaCollectionName", record.getValue());

            assertEquals(1, mongo.find(filteringQuery).size());
        }
    }

    @Test
    public void test_RetrieveSourceDocumentReferences_EmptyRecords() {
        assertTrue(europeanaDao.retrieveAggregationEDMInformation(Collections.<String, String>emptyMap(),migrationBatchId).isEmpty());
    }

    @Test
    public void test_RetrieveSourceDocumentReferences_OneRecords() {
        final String recordId = "/04202/BibliographicResource_2000068285812";
        final String collectionName = "04202_L_BE_UniLibGent_googlebooks";
        final Map<String, String> record = new HashMap<>();
        record.put(recordId, collectionName);

        final List<EuropeanaEDMObject> edmObjects = europeanaDao.retrieveAggregationEDMInformation(record,migrationBatchId);

        assertEquals (1, edmObjects.size());

        final EuropeanaEDMObject edmObject = edmObjects.get(0);
        assertNull (edmObject.getEdmIsShownBy());
        assertEquals ("http://search.ugent.be/meercat/x/bkt01?q=900000061304", edmObject.getEdmIsShownAt());
        assertEquals("http://bks1.books.google.be/books?vid=GENT900000061304&printsec=titlepage&img=1&zoom=1", edmObject.getEdmObject());
        assertNull(edmObject.getEdmHasViews());
    }

    @Test
    public void test_RetrieveSourceDocumentReferences_ManyRecords() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null,migrationBatchId);
        final Map<String, String> records= europeanaDao.retrieveRecordsIdsFromCursor(cursor, 10,migrationBatchId);
        final List<EuropeanaEDMObject> edmObjects = europeanaDao.retrieveAggregationEDMInformation(records,migrationBatchId);

        final DBCollection aggregationColl = mongoDBUtils.connectToSource().getCollection("Aggregation");

        assertEquals (10, edmObjects.size());
        try {
            for (final EuropeanaEDMObject edmObject : edmObjects) {
                final DBObject aggregation = aggregationColl.find(new BasicDBObject("about",
                                                                                    "/aggregation/provider" + edmObject.getReferenceOwner().getRecordId())).next();

                final BasicDBList hasViewList = (BasicDBList)aggregation.get("hasView");
                assertEquals(aggregation.get("edmIsShownAt"), edmObject.getEdmIsShownAt());
                assertEquals (aggregation.get("edmIsShownBy"), edmObject.getEdmIsShownBy());
                assertEquals(aggregation.get("edmObject"), edmObject.getEdmObject());


                if (null == hasViewList) {
                    assertNull (edmObject.getEdmHasViews());
                }
                else {
                    assertArrayEquals(edmObject.getEdmHasViews().toArray(), hasViewList.toArray());
                }
            }
        }
        catch (Exception e) {
            fail (e.getMessage());
        }
    }
}
