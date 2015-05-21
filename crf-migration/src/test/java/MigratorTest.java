import com.mongodb.*;
import com.mongodb.util.JSON;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.Migrator;
import eu.europeana.crfmigration.logic.MigratorMetrics;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by salexandru on 20.05.2015.
 */
public class MigratorTest {
    private MongoConfig mongoConfig;

    private static String PATH_PREFIX = "./src/test/java/";

    @After
    public void tearDown() {
      cleanMongoDatabase();
    }


    @Test
    public void testDBIteration_AllDatabase() {
        mongoConfig = createMigratorConfig(PATH_PREFIX + "config-files/allData/migration.conf");

        loadMongoData(PATH_PREFIX + "data-files/allData/record.json", "record");
        loadMongoData(PATH_PREFIX + "data-files/allData/aggregation.json", "Aggregation");

        runMigrator(mongoConfig, null);

        final DBCollection aggregation = connectToDB(mongoConfig.getSourceHost(), mongoConfig.getSourcePort(),
                                                mongoConfig.getSourceDBUsername(), mongoConfig.getSourceDBPassword()
                                               ).getDB(mongoConfig.getSourceDBName())
                                                .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                                  mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword()
                                                                 ).getDB(mongoConfig.getTargetDBName())
                                                                  .getCollection("SourceDocumentReference");

        final DBCollection processJobs = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                     mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword()
                                                    ).getDB(mongoConfig.getTargetDBName())
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
        mongoConfig = createMigratorConfig(PATH_PREFIX + "config-files/filterSomeByDate/migration.conf");

        loadMongoData(PATH_PREFIX + "data-files/filterSomeByDate/record.json", "record");
        loadMongoData(PATH_PREFIX + "data-files/filterSomeByDate/aggregation.json", "Aggregation");

        runMigrator(mongoConfig, DateTime.parse("2014-08-15T00:00:00.000Z").toDate());

        final DBCollection aggregation = connectToDB(mongoConfig.getSourceHost(), mongoConfig.getSourcePort(),
                                                     mongoConfig.getSourceDBUsername(), mongoConfig.getSourceDBPassword()
                                                    ).getDB(mongoConfig.getSourceDBName())
                                                     .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                                  mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword()
                                                                 ).getDB(mongoConfig.getTargetDBName())
                                                                  .getCollection("SourceDocumentReference");

        final DBCollection processJobs = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                     mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword()
                                                    ).getDB(mongoConfig.getTargetDBName())
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
        mongoConfig = createMigratorConfig(PATH_PREFIX + "config-files/filterNoOneByDate/migration.conf");

        loadMongoData(PATH_PREFIX + "data-files/filterNoOneByDate/record.json", "record");
        loadMongoData(PATH_PREFIX + "data-files/filterNoOneByDate/aggregation.json", "Aggregation");

        runMigrator(mongoConfig, DateTime.parse("2014-06-19T00:00:00.000Z").toDate());

        final DBCollection aggregation = connectToDB(mongoConfig.getSourceHost(), mongoConfig.getSourcePort(),
                                                     mongoConfig.getSourceDBUsername(), mongoConfig.getSourceDBPassword()
                                                    ).getDB(mongoConfig.getSourceDBName())
                                                     .getCollection("Aggregation");

        final DBCollection sourceDocumentReferences = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                                  mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword()
                                                                 ).getDB(mongoConfig.getTargetDBName())
                                                                  .getCollection("SourceDocumentReference");

        final DBCollection processJobs = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                     mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword()
                                                    ).getDB(mongoConfig.getTargetDBName())
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


        if (!mongoConfig.getSourceDBUsername().equals("")) {
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
            if (null == mongoConfig) {
                fail("Mongo Configuration cannot be null!");
            }

            final Mongo sourceMongo = connectToDB(mongoConfig.getSourceHost(),
                                                  mongoConfig.getSourcePort(),
                                                  mongoConfig.getSourceDBUsername(),
                                                  mongoConfig.getSourceDBPassword());

            sourceMongo.getDB(mongoConfig.getSourceDBName()).getCollection("record").drop();
            sourceMongo.getDB(mongoConfig.getSourceDBName()).getCollection("Aggregation").drop();


            final Mongo targetMongo = connectToDB(mongoConfig.getTargetHost(), mongoConfig.getTargetPort(),
                                                  mongoConfig.getTargetDBUsername(), mongoConfig.getTargetDBPassword());

            targetMongo.getDB(mongoConfig.getTargetDBName()).getCollection("ProcessingJob").drop();
            targetMongo.getDB(mongoConfig.getTargetDBName()).getCollection("SourceDocumentReference").drop();

        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private void loadMongoData(final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(mongoConfig.getSourceHost(),
                                                  mongoConfig.getSourcePort(),
                                                  mongoConfig.getSourceDBUsername(),
                                                  mongoConfig.getSourceDBPassword());
            final DBCollection sourceDB = sourceMongo.getDB(mongoConfig.getSourceDBName()).getCollection(collectionName);
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

    private MongoConfig createMigratorConfig(final String pathToConfigFile) {
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
        final String graphiteMasterId = config.getString("metrics.masterID");
        final String graphiteServer   = config.getString("metrics.graphiteServer");
        final Integer graphitePort    = config.getInt("metrics.graphitePort");

        return new MongoConfig(sourceHost, sourcePort, sourceDBName, sourceDBUsername, sourceDBPassword,
                               batch,
                               targetHost, targetPort, targetDBName, targetDBUsername, targetDBPassword,
                               graphiteMasterId, graphitePort, graphiteServer
                            );
    }

    private void runMigrator(final MongoConfig mongoConfig, final Date dateTimeFilter) {
        try {
            new Migrator(mongoConfig, dateTimeFilter).migrate();

            for (final String name: MigratorMetrics.metricRegistry.getNames()) {
                MigratorMetrics.metricRegistry.remove(name);
            }
        } catch (Exception e) {
            fail("IOException: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()) + "\n");
        }
    }

}
