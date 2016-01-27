package eu.europeana.crfmigration;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.ServerAddress;
import eu.europeana.crfmigration.domain.GraphiteReporterConfig;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.harvester.domain.MongoConfig;
import org.joda.time.DateTime;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import utils.MigratorUtils;
import utils.MongoDBUtils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Ignore
public class MigrationManagerTest {


    private final List<ServerAddress> servers = new ArrayList<ServerAddress>();

    public MigrationManagerTest() throws UnknownHostException {
        servers.add(new ServerAddress("127.0.0.1", 27017));
        servers.add(new ServerAddress("127.0.0.1", 27017));
    }


    @Test
    public void testDBIteration_AllDatabase() throws IOException, ParseException, InterruptedException, ExecutionException, TimeoutException {
        final MigratorConfig migratorConfig = new MigratorConfig(
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

        final MongoDBUtils mongoDBUtils = new MongoDBUtils(migratorConfig);

        mongoDBUtils.loadMongoData(MigratorUtils.PATH_PREFIX + "data-files/allData/record.json", "record");
        mongoDBUtils.loadMongoData(MigratorUtils.PATH_PREFIX + "data-files/allData/aggregation.json", "Aggregation");

        MigratorUtils.runMigrator(migratorConfig, null);

        final DBCollection aggregation = mongoDBUtils.connectToSource().getCollection("Aggregation");
        final DBCollection sourceDocumentReferences = mongoDBUtils.connectToTarget()
                .getCollection("SourceDocumentReference");
        final DBCollection processJobs = mongoDBUtils.connectToTarget().getCollection("ProcessingJob");


        final int count2 = aggregation.find(new BasicDBObject("edmObject", new BasicDBObject("$exists", true))).size() +
                aggregation.find(new BasicDBObject("hasView", new BasicDBObject("$exists", true)))
                        .size() +
                aggregation
                        .find(new BasicDBObject("edmIsShownBy", new BasicDBObject("$exists", true)))
                        .size() +
                aggregation
                        .find(new BasicDBObject("edmIsShownAt", new BasicDBObject("$exists", true)))
                        .size();

        assertEquals(20, sourceDocumentReferences.find().size());
        assertEquals(count2, processJobs.find().size());

        mongoDBUtils.cleanMongoDatabase();
    }

    @Test
    public void testDBIteration_FilterByDate_BatchSize2_AllMinus2() throws IOException, ParseException, InterruptedException, ExecutionException, TimeoutException {
        final MigratorConfig migratorConfig = new MigratorConfig(
                new MongoConfig(servers, "source_migration", "", ""),
                new MongoConfig(servers, "dest_migration", "", ""),
                new GraphiteReporterConfig("127.0.0.1", "test", 10000),
                2,
                new DateTime(
                        2000,
                        12,
                        30,
                        0,
                        20));


        final MongoDBUtils mongoDBUtils = new MongoDBUtils(migratorConfig.withBatchSize(2));

        mongoDBUtils.loadMongoData(MigratorUtils.PATH_PREFIX + "data-files/filterSomeByDate/record.json", "record");
        mongoDBUtils.loadMongoData(MigratorUtils.PATH_PREFIX + "data-files/filterSomeByDate/aggregation.json",
                "Aggregation");

        MigratorUtils.runMigrator(migratorConfig, DateTime.parse("2014-08-15T00:00:00.000Z").toDate());

        final DBCollection aggregation = mongoDBUtils.connectToSource().getCollection("Aggregation");
        final DBCollection sourceDocumentReferences = mongoDBUtils.connectToTarget()
                .getCollection("SourceDocumentReference");
        final DBCollection processJobs = mongoDBUtils.connectToTarget().getCollection("ProcessingJob");

        final BasicDBObject query = new BasicDBObject("timestampUpdated", new BasicDBObject("$lt", DateTime.parse("2014-08-15T00:00:00.000Z")
                .toDate()));

        aggregation.remove(query);

        assertEquals(12, sourceDocumentReferences.find().size());
        assertEquals(12, processJobs.find().size());
        mongoDBUtils.cleanMongoDatabase();
    }

    @Test
    public void testDBIteration_FilterByDate_BatchSize2_All() throws IOException, ParseException, InterruptedException, ExecutionException, TimeoutException {
        final MigratorConfig migratorConfig = new MigratorConfig(
                new MongoConfig(servers, "source_migration", "", ""),
                new MongoConfig(servers, "dest_migration", "", ""),
                new GraphiteReporterConfig("127.0.0.1", "test", 10000),
                2,
                new DateTime(
                        2000,
                        12,
                        30,
                        0,
                        20));

        final MongoDBUtils mongoDBUtils = new MongoDBUtils(migratorConfig.withBatchSize(2));

        mongoDBUtils.loadMongoData(MigratorUtils.PATH_PREFIX + "data-files/filterNoOneByDate/record.json", "record");
        mongoDBUtils.loadMongoData(MigratorUtils.PATH_PREFIX + "data-files/filterNoOneByDate/aggregation.json",
                "Aggregation");

        MigratorUtils.runMigrator(migratorConfig, DateTime.parse("2014-06-19T00:00:00.000Z").toDate());


        final DBCollection aggregation = mongoDBUtils.connectToSource().getCollection("Aggregation");
        final DBCollection sourceDocumentReferences = mongoDBUtils.connectToTarget()
                .getCollection("SourceDocumentReference");
        final DBCollection processJobs = mongoDBUtils.connectToTarget().getCollection("ProcessingJob");


        final int count2 = aggregation.find(new BasicDBObject("edmObject", new BasicDBObject("$exists", true))).size() +
                aggregation.find(new BasicDBObject("hasView", new BasicDBObject("$exists", true)))
                        .size() +
                aggregation
                        .find(new BasicDBObject("edmIsShownBy", new BasicDBObject("$exists", true)))
                        .size() +
                aggregation
                        .find(new BasicDBObject("edmIsShownAt", new BasicDBObject("$exists", true)))
                        .size();

        assertEquals(20, sourceDocumentReferences.find().size());
        assertEquals(count2, processJobs.find().size());
        mongoDBUtils.cleanMongoDatabase();
    }


}
