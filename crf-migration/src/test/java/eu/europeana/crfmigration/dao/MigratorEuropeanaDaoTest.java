package eu.europeana.crfmigration.dao;

import com.mongodb.*;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.crfmigration.logic.MigratorMetrics;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utils.MigratorUtils;
import utils.MongoDBUtils;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static utils.MigratorUtils.*;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MigratorEuropeanaDaoTest {
    private final static MigratorConfig migratorConfig = MigratorUtils.createMigratorConfig("config-files/migration.conf");

    private final static Date dateFilter = DateTime.parse("2014-08-15T00:00:00.000Z").toDate();

    private MongoDBUtils mongoDBUtils;
    private MigratorEuropeanaDao europeanaDao;

    @Before
    public void setUp() throws UnknownHostException {
        mongoDBUtils = new MongoDBUtils(migratorConfig);
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/aggregation.json", "Aggregation");
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/record.json", "record");
        europeanaDao = new MigratorEuropeanaDao(migratorConfig.getSourceMongoConfig(), new MigratorMetrics());
    }

    @After
    public void tearDown() {
    //   mongoDBUtils.cleanMongoDatabase();
    }

    @Test
    public void test_GenerateCursor_NoFiltering() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null);
        assertEquals(new BasicDBObject(), cursor.getQuery());
    }

    @Test
    public void test_GenerateCursor_DateFiltering() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter);
        assertEquals (new BasicDBObject("timestampUpdated", new BasicDBObject("$gt", dateFilter)), cursor.getQuery());
    }

    @Test
    public void test_RetrieveData_NoFiltering_BatchSizeAllDB() {
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(null);
        final DBCollection mongo = mongoDBUtils.connectToSource().getCollection("record");
        final Map<String, String> records = europeanaDao.retrieveRecordsIdsFromCursor(cursor, mongo.find().size());

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
        final DBCursor cursor = europeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter);
        final DBCollection mongo = mongoDBUtils.connectToSource().getCollection("record");
        final Map<String, String> records = europeanaDao.retrieveRecordsIdsFromCursor(cursor, mongo.find(query).size());

        assertEquals(mongo.find(query).size(), records.size());
        for (final Map.Entry<String, String> record: records.entrySet()) {
            final BasicDBObject filteringQuery = new BasicDBObject(query);
            filteringQuery.put("about", record.getKey());
            filteringQuery.put("europeanaCollectionName", record.getValue());

            assertEquals(1, mongo.find(filteringQuery).size());
        }
    }
}
