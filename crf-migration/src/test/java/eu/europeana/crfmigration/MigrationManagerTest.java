package eu.europeana.crfmigration;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.MigrationManager;
import eu.europeana.crfmigration.logic.MigratorMetrics;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MigrationManagerTest {
    private MigratorConfig migratorConfig;

    private static String PATH_PREFIX = "./src/test/java/";

    @After
    public void tearDown() {
      cleanMongoDatabase();
    }


    @Test
    public void testDBIteration_AllDatabase() {
        migratorConfig = createMigratorConfig(PATH_PREFIX + "src/test/resources/config-files/allData/migration.conf");

        loadMongoData(PATH_PREFIX + "src/test/resources/data-files/allData/record.json", "record");
        loadMongoData(PATH_PREFIX + "src/test/resources/data-files/allData/aggregation.json", "Aggregation");

        runMigrator(migratorConfig, null);

        final DBCollection aggregation = connectToDB(migratorConfig.getSourceMongoConfig().getHost(), migratorConfig.getSourceMongoConfig().getPort(),
                                                migratorConfig.getSourceMongoConfig().getdBUsername(), migratorConfig.getSourceMongoConfig().getdBPassword()
                                               ).getDB(migratorConfig.getSourceMongoConfig().getdBName())
                                                .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                                                                  migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword()
                                                                 ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                                                                  .getCollection("SourceDocumentReference");

        final DBCollection processJobs = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                                                     migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword()
                                                    ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                                                     .getCollection("ProcessingJobs");

        final DBCursor sourceDocumentReferencesCursor = sourceDocumentReferences.find();
        final DBCursor processJobsCursor = processJobs.find();

        final int count = aggregation.distinct("edmObject").size() + aggregation.distinct("hasView").size() +
                          aggregation.distinct("edmIsShownBy").size() + aggregation.distinct("edmIsShownAt").size();

        assertEquals (count, sourceDocumentReferencesCursor.size());
      //  assertEquals(-1, processJobsCursor.size());
    }

    @Test
    public void testDBIteration_FilterByDate_BatchSize2_AllMinus2() {
        migratorConfig = createMigratorConfig(PATH_PREFIX + "src/test/resources/config-files/filterSomeByDate/migration.conf");

        loadMongoData(PATH_PREFIX + "src/test/resources/data-files/filterSomeByDate/record.json", "record");
        loadMongoData(PATH_PREFIX + "src/test/resources/data-files/filterSomeByDate/aggregation.json", "Aggregation");

        runMigrator(migratorConfig, DateTime.parse("2014-08-15T00:00:00.000Z").toDate());

        final DBCollection aggregation = connectToDB(migratorConfig.getSourceMongoConfig().getHost(), migratorConfig.getSourceMongoConfig().getPort(),
                migratorConfig.getSourceMongoConfig().getdBUsername(), migratorConfig.getSourceMongoConfig().getdBPassword()
        ).getDB(migratorConfig.getSourceMongoConfig().getdBName())
                .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword()
        ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                .getCollection("SourceDocumentReference");

        final DBCollection processJobs = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword()
        ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                .getCollection("ProcessingJobs");

        final DBCursor sourceDocumentReferencesCursor = sourceDocumentReferences.find();
        final DBCursor processJobsCursor = processJobs.find();
        final BasicDBObject query = new BasicDBObject("timestampUpdated",
                                                      new BasicDBObject("$lt", DateTime.parse("2014-08-15T00:00:00.000Z").toDate()));

        aggregation.remove(query);

        final int count = aggregation.distinct("edmObject").size() + aggregation.distinct("hasView").size() +
                          aggregation.distinct("edmIsShownBy").size() + aggregation.distinct("edmIsShownAt").size();
        assertEquals (count, sourceDocumentReferencesCursor.size());
     //   assertEquals(-1, processJobsCursor.size());
    }

    @Test
    public void testDBIteration_FilterByDate_BatchSize2_All() {
        migratorConfig = createMigratorConfig(PATH_PREFIX + "src/test/resources/config-files/filterNoOneByDate/migration.conf");

        loadMongoData(PATH_PREFIX + "src/test/resources/data-files/filterNoOneByDate/record.json", "record");
        loadMongoData(PATH_PREFIX + "src/test/resources/data-files/filterNoOneByDate/aggregation.json", "Aggregation");

        runMigrator(migratorConfig, DateTime.parse("2014-06-19T00:00:00.000Z").toDate());


        final DBCollection aggregation = connectToDB(migratorConfig.getSourceMongoConfig().getHost(), migratorConfig.getSourceMongoConfig().getPort(),
                migratorConfig.getSourceMongoConfig().getdBUsername(), migratorConfig.getSourceMongoConfig().getdBPassword()
        ).getDB(migratorConfig.getSourceMongoConfig().getdBName())
                .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword()
        ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                .getCollection("SourceDocumentReference");

        final DBCollection processJobs = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword()
        ).getDB(migratorConfig.getTargetMongoConfig().getdBName())
                .getCollection("ProcessingJobs");

        final DBCursor sourceDocumentReferencesCursor = sourceDocumentReferences.find();
        final DBCursor processJobsCursor = processJobs.find();

        System.out.println(aggregation.find().size());


        final int count = aggregation.distinct("edmObject").size() + aggregation.distinct("hasView").size() +
                          aggregation.distinct("edmIsShownBy").size() + aggregation.distinct("edmIsShownAt").size();

        assertEquals (count, sourceDocumentReferencesCursor.size());
       // assertEquals(-1, processJobsCursor.size());
    }


    private Mongo connectToDB(final String host, final Integer port, final String userName, final String password) {
        final Mongo mongo;
        try {
            mongo = new Mongo(host, port);
        } catch (UnknownHostException e) {
            fail("Cannot connect to mongo database. Unknown host: " + e.getMessage());
            return null;
        }


        if (!migratorConfig.getSourceMongoConfig().getdBUsername().equals("")) {
            final DB sourceDB = mongo.getDB("admin");
            final Boolean auth = sourceDB.authenticate(userName, password.toCharArray());
            if (!auth) {
                fail("Mongo source auth error");
            }
        }

        return mongo;
    }

    private void cleanMongoDatabase() {
        try {
            if (null == migratorConfig) {
                fail("Mongo Configuration cannot be null!");
            }

            final Mongo sourceMongo = connectToDB(migratorConfig.getSourceMongoConfig().getHost(),
                                                  migratorConfig.getSourceMongoConfig().getPort(),
                                                  migratorConfig.getSourceMongoConfig().getdBUsername(),
                                                  migratorConfig.getSourceMongoConfig().getdBPassword());

            sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getdBName()).getCollection("record").drop();
            sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getdBName()).getCollection("Aggregation").drop();


            final Mongo targetMongo = connectToDB(migratorConfig.getTargetMongoConfig().getHost(), migratorConfig.getTargetMongoConfig().getPort(),
                                                  migratorConfig.getTargetMongoConfig().getdBUsername(), migratorConfig.getTargetMongoConfig().getdBPassword());

            targetMongo.getDB(migratorConfig.getTargetMongoConfig().getdBName()).getCollection("ProcessingJob").drop();
            targetMongo.getDB(migratorConfig.getTargetMongoConfig().getdBName()).getCollection("SourceDocumentReference").drop();

        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private void loadMongoData(final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(migratorConfig.getSourceMongoConfig().getHost(),
                    migratorConfig.getSourceMongoConfig().getPort(),
                    migratorConfig.getSourceMongoConfig().getdBUsername(),
                    migratorConfig.getSourceMongoConfig().getdBPassword());
            final DBCollection sourceDB = sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getdBName()).getCollection(collectionName);
            for (final Object object : rootObject) {
                final DBObject dbObject = (DBObject) JSON.parse(object.toString());

                if (dbObject.containsField("timestampCreated")) {
                    dbObject.put("timestampCreated", DateTime.parse(dbObject.get("timestampCreated").toString()).toDate());
                }
                if (dbObject.containsField("timestampUpdated")) {
                    dbObject.put("timestampUpdated", DateTime.parse(dbObject.get("timestampUpdated").toString()).toDate());
                }

                sourceDB.save(dbObject);
            }
        } catch (Exception e) {
            fail("Failed to load data to mongo\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private MigratorConfig createMigratorConfig(final String pathToConfigFile) {
        final File configFile = new File(pathToConfigFile);

        if (!configFile.canRead()) {
            fail("Cannot read file " + pathToConfigFile);
            return null;
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile, ConfigParseOptions.defaults()
                                                                                             .setSyntax(ConfigSyntax
                                                                                                                .CONF));

        final Integer batch = config.getInt("config.batch");

        final Config sourceMongoConfig = config.getConfig("sourceMongo");
        final Config targetMongoConfig = config.getConfig("targetMongo");

        final String sourceHost = sourceMongoConfig.getString("host");
        final Integer sourcePort = sourceMongoConfig.getInt("port");
        final String sourceDBName = sourceMongoConfig.getString("dbName");
        final String sourceDBUsername = sourceMongoConfig.getString("username");
        final String sourceDBPassword = sourceMongoConfig.getString("password");

        final String targetHost = targetMongoConfig.getString("host");
        final Integer targetPort = targetMongoConfig.getInt("port");
        final String targetDBName = targetMongoConfig.getString("dbName");
        final String targetDBUsername = targetMongoConfig.getString("username");
        final String targetDBPassword = targetMongoConfig.getString("password");

        final MongoConfig sourceMongo = new MongoConfig(sourceHost, sourcePort, sourceDBName, sourceDBUsername, sourceDBPassword);
        final MongoConfig targetMongo = new MongoConfig(targetHost, targetPort, targetDBName, targetDBUsername, targetDBPassword);

        final MigratorConfig migrationConfig = new MigratorConfig(sourceMongo, targetMongo, null, batch);

        return migrationConfig;
    }

    private void runMigrator(final MigratorConfig migratorConfig, final Date dateFilter) {
        final MigratorMetrics metrics = new MigratorMetrics();
        try {


            final MigratorEuropeanaDao migratorEuropeanaDao = new MigratorEuropeanaDao(migratorConfig.getSourceMongoConfig(), metrics);
            final MigratorHarvesterDao migratorHarvesterDao = new MigratorHarvesterDao(metrics, migratorConfig.getTargetMongoConfig());

            // Prepare the migrator & start it
            final MigrationManager migrationManager = new MigrationManager(migratorEuropeanaDao,
                    migratorHarvesterDao,
                    metrics,
                    dateFilter,
                    migratorConfig.getBatch());

            migrationManager.migrate();
        } catch (Exception e) {
            fail("IOException: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()) + "\n");
        }
        finally {
            for (final String name: MigratorMetrics.metricRegistry.getNames()) {
                MigratorMetrics.metricRegistry.remove(name);
            }
        }
    }

}
