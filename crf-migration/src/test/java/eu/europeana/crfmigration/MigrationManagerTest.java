package eu.europeana.crfmigration;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import eu.europeana.crfmigration.domain.MigratorConfig;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;
import utils.MigratorUtils;
import utils.MongoDBUtils;


import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class MigrationManagerTest {
    private MigratorConfig migratorConfig;
    private MongoDBUtils mongoDBUtils;

    private static String PATH_PREFIX = "./src/test/java/";

    @After
    public void tearDown() {
      mongoDBUtils.cleanMongoDatabase();
    }


    @Test
    public void testDBIteration_AllDatabase() throws IOException {
        migratorConfig = MigratorUtils.createMigratorConfig(PATH_PREFIX + "config-files/allData/migration.conf");
        mongoDBUtils = new MongoDBUtils(migratorConfig);

        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/allData/record.json", "record");
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/allData/aggregation.json", "Aggregation");

        MigratorUtils.runMigrator(migratorConfig, null);

        final DBCollection aggregation = mongoDBUtils.connectToDB(migratorConfig.getSourceMongoConfig().getHost(),
                                                                  migratorConfig.getSourceMongoConfig().getPort(),
                                                                  migratorConfig.getSourceMongoConfig().getdBUsername(),
                                                                  migratorConfig.getSourceMongoConfig().getdBPassword()).getDB(migratorConfig.getSourceMongoConfig().getdBName())
                                                .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = mongoDBUtils.connectToDB(migratorConfig.getTargetMongoConfig()
                                                                                             .getHost(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getPort(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getdBUsername(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getdBPassword()).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                                                                  .getCollection("SourceDocumentReference");

        final DBCollection processJobs = mongoDBUtils.connectToDB(migratorConfig.getTargetMongoConfig().getHost(),
                                                                  migratorConfig.getTargetMongoConfig().getPort(),
                                                                  migratorConfig.getTargetMongoConfig().getdBUsername(),
                                                                  migratorConfig.getTargetMongoConfig().getdBPassword()).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                                                     .getCollection("ProcessingJob");

        final int count = aggregation.distinct("edmObject").size() + aggregation.distinct("hasView").size() +
                          aggregation.distinct("edmIsShownBy").size() + aggregation.distinct("edmIsShownAt").size();
        final int count2 = aggregation.find(new BasicDBObject("edmObject", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("hasView", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("edmIsShownBy", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("edmIsShownAt", new BasicDBObject("$exists", true))).size();
        assertEquals(count, sourceDocumentReferences.find().size());
        assertEquals(count2, processJobs.find().size());
    }

    @Test
    public void testDBIteration_FilterByDate_BatchSize2_AllMinus2() throws IOException {
        migratorConfig = MigratorUtils.createMigratorConfig(PATH_PREFIX + "config-files/filterSomeByDate/migration.conf");
        mongoDBUtils = new MongoDBUtils(migratorConfig);

        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/filterSomeByDate/record.json", "record");
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/filterSomeByDate/aggregation.json", "Aggregation");

        MigratorUtils.runMigrator(migratorConfig, DateTime.parse("2014-08-15T00:00:00.000Z").toDate());

        final DBCollection aggregation = mongoDBUtils.connectToDB(migratorConfig.getSourceMongoConfig().getHost(),
                                                                  migratorConfig.getSourceMongoConfig().getPort(),
                                                                  migratorConfig.getSourceMongoConfig().getdBUsername(),
                                                                  migratorConfig.getSourceMongoConfig().getdBPassword()).getDB(migratorConfig.getSourceMongoConfig().getdBName())
                .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = mongoDBUtils.connectToDB(migratorConfig.getTargetMongoConfig()
                                                                                             .getHost(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getPort(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getdBUsername(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getdBPassword()).getDB(migratorConfig
                                                                                                                             .getTargetMongoConfig()
                                                                                                                             .getdBName())
                .getCollection("SourceDocumentReference");

        final DBCollection processJobs = mongoDBUtils.connectToDB(migratorConfig.getTargetMongoConfig().getHost(),
                                                                  migratorConfig.getTargetMongoConfig().getPort(),
                                                                  migratorConfig.getTargetMongoConfig().getdBUsername(),
                                                                  migratorConfig.getTargetMongoConfig().getdBPassword())
                                                     .getDB(migratorConfig.getTargetMongoConfig().getdBName())
                .getCollection("ProcessingJob");

        final BasicDBObject query = new BasicDBObject("timestampUpdated",
                                                      new BasicDBObject("$lt", DateTime.parse("2014-08-15T00:00:00.000Z").toDate()));

        aggregation.remove(query);

        final int count = aggregation.distinct("edmObject").size() + aggregation.distinct("hasView").size() +
                          aggregation.distinct("edmIsShownBy").size() + aggregation.distinct("edmIsShownAt").size();
        final int count2 = aggregation.find(new BasicDBObject("edmObject", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("hasView", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("edmIsShownBy",
                                                                      new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("edmIsShownAt",
                                                                      new BasicDBObject("$exists", true))).size();
        assertEquals (count, sourceDocumentReferences.find().size());
        assertEquals(count2, processJobs.find().size());
    }

    @Test
    public void testDBIteration_FilterByDate_BatchSize2_All() throws IOException {
        migratorConfig = MigratorUtils.createMigratorConfig(PATH_PREFIX + "config-files/filterNoOneByDate/migration" +
                                                                    ".conf");
        mongoDBUtils = new MongoDBUtils(migratorConfig);

        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/filterNoOneByDate/record.json", "record");
        mongoDBUtils.loadMongoData(PATH_PREFIX + "data-files/filterNoOneByDate/aggregation.json", "Aggregation");

        MigratorUtils.runMigrator(migratorConfig, DateTime.parse("2014-06-19T00:00:00.000Z").toDate());


        final DBCollection aggregation = mongoDBUtils.connectToDB(migratorConfig.getSourceMongoConfig().getHost(),
                                                                  migratorConfig.getSourceMongoConfig().getPort(),
                                                                  migratorConfig.getSourceMongoConfig().getdBUsername(),
                                                                  migratorConfig.getSourceMongoConfig().getdBPassword()
                                                                 ).getDB(migratorConfig.getSourceMongoConfig().getdBName())
                                                                  .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = mongoDBUtils.connectToDB(migratorConfig.getTargetMongoConfig()
                                                                                             .getHost(),
                                                                               migratorConfig.getTargetMongoConfig()
                                                                                             .getPort(), migratorConfig.getTargetMongoConfig()
                                                                                             .getdBUsername(), migratorConfig.getTargetMongoConfig()
                                                                                             .getdBPassword()).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                                                                               .getCollection("SourceDocumentReference");

        final DBCollection processJobs = mongoDBUtils.connectToDB(migratorConfig.getTargetMongoConfig().getHost(),
                                                                  migratorConfig.getTargetMongoConfig().getPort(),
                                                                  migratorConfig.getTargetMongoConfig().getdBUsername(),
                                                                  migratorConfig.getTargetMongoConfig().getdBPassword()
                                                                 ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                                                                  .getCollection("ProcessingJob");




        final int count = aggregation.distinct("edmObject").size() + aggregation.distinct("hasView").size() +
                          aggregation.distinct("edmIsShownBy").size() + aggregation.distinct("edmIsShownAt").size();
        final int count2 = aggregation.find(new BasicDBObject("edmObject", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("hasView", new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("edmIsShownBy",
                                                                      new BasicDBObject("$exists", true))).size() +
                                   aggregation.find(new BasicDBObject("edmIsShownAt",
                                                                      new BasicDBObject("$exists", true))).size();

        assertEquals (count, sourceDocumentReferences.find().size());
        assertEquals(count2, processJobs.find().size());
    }






}
