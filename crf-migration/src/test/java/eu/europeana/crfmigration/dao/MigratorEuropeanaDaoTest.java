package eu.europeana.crfmigration.dao;

import com.mongodb.*;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.crfmigration.domain.EuropeanaRecord;
import eu.europeana.crfmigration.domain.GraphiteReporterConfig;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.MongoDBUtils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
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
    public void setUp() throws IOException, ParseException {
        final List<ServerAddress> servers = new ArrayList<ServerAddress>();
        servers.add(new ServerAddress("127.0.0.1",27017));
        servers.add(new ServerAddress("127.0.0.1",27017));

        migratorConfig = new MigratorConfig(
                new MongoConfig(servers, "source_migration", "", ""),
                new MongoConfig(servers, "dest_migration", "", ""),
                new GraphiteReporterConfig("127.0.0.1", "test", 10000),
                2,
                new DateTime(
                        2015,
                        12,
                        30,
                        0,
                        10));

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
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null,1,migrationBatchId);
        assertEquals(new BasicDBObject(), cursor.getQuery());
    }

    @Test
    public void test_GenerateCursor_DateFiltering() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter,1,migrationBatchId);
        assertEquals (new BasicDBObject("timestampUpdated", new BasicDBObject("$gt", dateFilter)), cursor.getQuery());
    }

    @Test
    public void test_RetrieveData_NoFiltering_BatchSizeAllDB() throws IOException {
        final DBCollection mongo = mongoDBUtils.connectToSource().getCollection("record");
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null, mongo.find().size(), migrationBatchId);
        final Map<String, EuropeanaRecord> records = europeanaDao.retrieveRecordsIdsFromCursor(cursor,migrationBatchId);

        assertEquals (mongo.find().size(), records.size());
        for (final Map.Entry<String, EuropeanaRecord> record: records.entrySet()) {
            final BasicDBObject query = new BasicDBObject();
            query.put("about", record.getKey());
            query.put("europeanaCollectionName", record.getValue().getCollectionId());
            assertEquals(1, mongo.find(query).size());
        }
    }

    @Test
    public void test_RetrieveData_DateFiltering() throws IOException {
        final BasicDBObject query = new BasicDBObject("timestampUpdated", new BasicDBObject("$gt", dateFilter));
        final DBCollection mongo = mongoDBUtils.connectToSource().getCollection("record");
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter, mongo.find(query).size(), migrationBatchId);
        final Map<String, EuropeanaRecord> records = europeanaDao.retrieveRecordsIdsFromCursor(cursor, migrationBatchId);

        assertEquals(mongo.find(query).size(), records.size());
        for (final Map.Entry<String, EuropeanaRecord> record: records.entrySet()) {
            final BasicDBObject filteringQuery = new BasicDBObject(query);
            filteringQuery.put("about", record.getKey());
            filteringQuery.put("europeanaCollectionName", record.getValue().getCollectionId());

            assertEquals(1, mongo.find(filteringQuery).size());
        }
    }

    @Test
    public void test_RetrieveSourceDocumentReferences_EmptyRecords() {
        assertTrue(europeanaDao.retrieveAggregationEDMInformation(Collections.<String, EuropeanaRecord>emptyMap(),migrationBatchId).isEmpty());
    }

    @Test
    public void test_RetrieveSourceDocumentReferences_OneRecords() {
        final String recordId = "/04202/BibliographicResource_2000068285812";
        final String collectionName = "04202_L_BE_UniLibGent_googlebooks";
        final Map<String, EuropeanaRecord> record = new HashMap<>();
        record.put(recordId, new EuropeanaRecord(recordId,"",collectionName, new Date()));

        final List<EuropeanaEDMObject> edmObjects = europeanaDao.retrieveAggregationEDMInformation(record,migrationBatchId);

        assertEquals (1, edmObjects.size());

        final EuropeanaEDMObject edmObject = edmObjects.get(0);
        assertNull (edmObject.getEdmIsShownBy());
        assertEquals ("http://search.ugent.be/meercat/x/bkt01?q=900000061304", edmObject.getEdmIsShownAt());
        assertEquals("http://bks1.books.google.be/books?vid=GENT900000061304&printsec=titlepage&img=1&zoom=1", edmObject.getEdmObject());
        assertNull(edmObject.getEdmHasViews());
    }

    @Test
    public void test_RetrieveSourceDocumentReferences_ManyRecords() throws IOException {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null,1000,migrationBatchId);
        final Map<String, EuropeanaRecord> records= europeanaDao.retrieveRecordsIdsFromCursor(cursor,migrationBatchId);
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
