package eu.europeana.publisher.tests.integration;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.typesafe.config.*;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.Publisher;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class PublishingTests  {
    private PublisherConfig publisherConfig;

    @After
    private void tearDown() {
        cleanMongoDatabase();
        cleanSolrDatabase();
    }

    private Mongo connectToDB(final String host, final Integer port, final String userName, final String password) {
        final Mongo mongo;
        try {
            mongo = new Mongo(host, port);
        } catch (UnknownHostException e) {
            fail("Cannot connect to mongo database. Unkown host: " + e.getMessage());
            return null;
        }


        if (!publisherConfig.getSourceDBUsername().equals("")) {
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
            if (null == publisherConfig) {
                fail("Publisher Configuration cannot be null!");
            }

            final Mongo sourceMongo = connectToDB(publisherConfig.getSourceHost(),
                                                  publisherConfig.getSourcePort(),
                                                  publisherConfig.getSourceDBUsername(),
                                                  publisherConfig.getSourceDBPassword());

            for (final String collectionName : sourceMongo.getDB(publisherConfig.getSourceDBName()).getCollectionNames()) {
                sourceMongo.getDB(publisherConfig.getSourceDBName()).getCollection(collectionName).drop();
            }

            final Mongo targetMongo = connectToDB(publisherConfig.getTargetHost(),
                                                  publisherConfig.getTargetPort(),
                                                  publisherConfig.getTargetDBUsername(),
                                                  publisherConfig.getTargetDBPassword());

            for (final String collectionName : targetMongo.getDB(publisherConfig.getTargetDBName()).getCollectionNames()) {
                targetMongo.getDB(publisherConfig.getTargetDBName()).getCollection(collectionName).drop();
            }
        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private void cleanSolrDatabase() {
        try {
            if (null == publisherConfig) {
                fail("Publisher Configuration cannot be null");
            }

            final SolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());
            solrServer.deleteByQuery("*:*");
            solrServer.commit();
        }
        catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }


    private void loadMongoData (final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray)parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(publisherConfig.getSourceHost(),
                                                  publisherConfig.getSourcePort(),
                                                  publisherConfig.getSourceDBUsername(),
                                                  publisherConfig.getSourceDBPassword());
            final DBCollection sourceDB = sourceMongo.getDB(publisherConfig.getSourceDBName()).getCollection(collectionName);
            for (final Object object: rootObject) {
                sourceDB.save((DBObject)JSON.parse(object.toString()));
            }
        }
        catch (Exception e) {
            fail("Failed to load data to mongo\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private PublisherConfig createPublisherConfig(final String pathToConfigFile) {
        final File configFile = new File(pathToConfigFile);

        if (!configFile.canRead()) {
            fail("Cannot read file " + pathToConfigFile);
            return null;
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                                                               ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        DateTime startTimestamp = null;
        try {
            startTimestamp = DateTime.parse(config.getString("criteria.startTimestamp"));
        }
        catch (ConfigException.Null e) {
        }


        String startTimestampFile = null;
        try {
            startTimestampFile = config.getString("criteria.startTimestampFile");
            if (null != startTimestampFile) {
                String file = FileUtils.readFileToString(new File(startTimestampFile), Charset.forName("UTF-8").name());
                if (!StringUtils.isEmpty(file)) {
                    startTimestamp = DateTime.parse(file.trim());
                }
            }
        }
        catch (ConfigException.Null e) {
        }
        catch (FileNotFoundException e) {
        } catch (IOException e) {
            fail("Fail to load startTimestampFile: " + startTimestampFile + "\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()) + "\n");
            return null;
        }

        final String solrURL = config.getString("solr.url");

        final Integer batch = config.getInt("config.batch");

        final Config sourceMongoConfig = config.getConfigList("sourceMongo").get(0);
        final Config targetMongoConfig = config.getConfigList("targetMongo").get(0);

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

        return new PublisherConfig(sourceHost,
                                   sourcePort,
                                   sourceDBName,
                                   sourceDBUsername,
                                   sourceDBPassword,
                                   targetHost,
                                   targetPort,
                                   targetDBName,
                                   targetDBUsername,
                                   targetDBPassword,
                                   startTimestamp,
                                   startTimestampFile,
                                   solrURL,
                                   batch);
    }

    private void runPublisher(final PublisherConfig publisherConfig) {
        try {
            new Publisher(publisherConfig).start();
        } catch (SolrServerException e) {
            fail("SolrServerException: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()) + "\n");
        } catch (IOException e) {
            fail("IOException: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()) + "\n");
        }
    }

    @Test
    public void testPublishAllData() {
        publisherConfig = createPublisherConfig("config-files/validData/publisher.conf");
        loadMongoData("data-files/validData/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData("data-files/validData/metaInfo.json", "SourceDocumentReferenceMetaInfo");

        runPublisher(publisherConfig);

        final DBCollection sourceMetaInfoDB = connectToDB(publisherConfig.getSourceHost(),
                                                          publisherConfig.getSourcePort(),
                                                          publisherConfig.getSourceDBUsername(),
                                                          publisherConfig.getSourceDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = connectToDB(publisherConfig.getTargetHost(),
                                                                 publisherConfig.getTargetPort(),
                                                                 publisherConfig.getTargetDBUsername(),
                                                                 publisherConfig.getTargetDBPassword()
                                                          ).getDB(publisherConfig.getSourceDBName())
                                                           .getCollection("WebResourceMetaInfo");


        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), new BasicDBObject("className", 0)).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());
    }

    @Test
    public void testPublishAllDataAfterDate() {
        publisherConfig = createPublisherConfig("config-files/filterDataByDate/publisher.conf");
        loadMongoData("data-files/filterDataByDate/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData("data-files/filterDataByDate/metaInfo.json", "SourceDocumentReferenceMetaInfo");

        runPublisher(publisherConfig);
        final DBCollection sourceMetaInfoDB = connectToDB(publisherConfig.getSourceHost(),
                                                                 publisherConfig.getSourcePort(),
                                                                 publisherConfig.getSourceDBUsername(),
                                                                 publisherConfig.getSourceDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = connectToDB(publisherConfig.getTargetHost(),
                                                                 publisherConfig.getTargetPort(),
                                                                 publisherConfig.getTargetDBUsername(),
                                                                 publisherConfig.getTargetDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("WebResourceMetaInfo");


        final BasicDBObject findQuery = new BasicDBObject("updatedAt", new BasicDBObject("$gt", publisherConfig.getStartTimestamp().toDate()));
        final DBCursor sourceResults = sourceMetaInfoDB.find(findQuery, new BasicDBObject("className", 0)).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());
    }

    @Test
    public void testPublishAllDataWithMetaData() {
        publisherConfig = createPublisherConfig("config-files/dataWithMissingMetaInfo/publisher.conf");
        loadMongoData("data-files/dataWithMissingMetaInfo/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData("data-files/dataWithMissingMetaInfo/metaInfo.json", "SourceDocumentReferenceMetaInfo");


        runPublisher(publisherConfig);
        final DBCollection sourceMetaInfoDB = connectToDB(publisherConfig.getSourceHost(),
                                                                 publisherConfig.getSourcePort(),
                                                                 publisherConfig.getSourceDBUsername(),
                                                                 publisherConfig.getSourceDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = connectToDB(publisherConfig.getTargetHost(),
                                                                 publisherConfig.getTargetPort(),
                                                                 publisherConfig.getTargetDBUsername(),
                                                                 publisherConfig.getTargetDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("WebResourceMetaInfo");


        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), new BasicDBObject("className", 0)).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());
    }

    @Test
    @Ignore
    public void testPublishAllDataWithSuccessStatus() {
        publisherConfig = createPublisherConfig("config-files/dataFromSuccessJobs/publisher.conf");
        loadMongoData("data-files/dataFromSuccessJobs/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData("data-files/dataFromSuccessJobs/metaInfo.json", "SourceDocumentReferenceMetaInfo");

        runPublisher(publisherConfig);
        final DBCollection sourceMetaInfoDB = connectToDB(publisherConfig.getSourceHost(),
                                                                 publisherConfig.getSourcePort(),
                                                                 publisherConfig.getSourceDBUsername(),
                                                                 publisherConfig.getSourceDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("SourceDocumentReferenceMetaInfo");


        final DBCollection targetMetaInfoDB = connectToDB(publisherConfig.getTargetHost(),
                                                                 publisherConfig.getTargetPort(),
                                                                 publisherConfig.getTargetDBUsername(),
                                                                 publisherConfig.getTargetDBPassword()
                                                         ).getDB(publisherConfig.getSourceDBName())
                                                          .getCollection("WebResourceMetaInfo");


        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), new BasicDBObject("className", 0)).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());
    }








}
