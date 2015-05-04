package eu.europeana.publisher.tests.integration;

import com.mongodb.*;
import com.mongodb.util.JSON;
import com.typesafe.config.*;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.CommonTagExtractor;
import eu.europeana.publisher.logic.MediaTypeEncoding;
import eu.europeana.publisher.logic.Publisher;
import eu.europeana.publisher.tests.integration.inverseLogic.CommonPropertyExtractor;
import eu.europeana.publisher.tests.integration.inverseLogic.ImagePropertyExtractor;
import eu.europeana.publisher.tests.integration.inverseLogic.SoundPropertyExtractor;
import eu.europeana.publisher.tests.integration.inverseLogic.VideoPropertyExtractor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.lang.model.type.ArrayType;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@Category(IntegrationTest.class)
public class PublishingTests {
    private PublisherConfig publisherConfig;

    private static String PATH_PREFIX = "./src/main/java/eu/europeana/publisher/tests/integration/";

    @After
    public void tearDown() {
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

            sourceMongo.getDB(publisherConfig.getSourceDBName()).getCollection("SourceDocumentProcessingStatistics").drop();
            sourceMongo.getDB(publisherConfig.getSourceDBName()).getCollection("SourceDocumentReferenceMetaInfo").drop();


            final Mongo targetMongo = connectToDB(publisherConfig.getTargetHost(),
                    publisherConfig.getTargetPort(),
                    publisherConfig.getTargetDBUsername(),
                    publisherConfig.getTargetDBPassword());

            targetMongo.getDB(publisherConfig.getTargetDBName()).getCollection("WebResourceMetaInfo").drop();
            targetMongo.getDB(publisherConfig.getTargetDBName()).getCollection("WebResource").drop();

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
        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }


    private void loadMongoData(final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(publisherConfig.getSourceHost(),
                    publisherConfig.getSourcePort(),
                    publisherConfig.getSourceDBUsername(),
                    publisherConfig.getSourceDBPassword());
            final DBCollection sourceDB = sourceMongo.getDB(publisherConfig.getSourceDBName()).getCollection(collectionName);
            for (final Object object : rootObject) {
                final DBObject dbObject = (DBObject) JSON.parse(object.toString());

                if (dbObject.containsField("updatedAt")) {
                    dbObject.put("updatedAt", DateTime.parse(dbObject.get("updatedAt").toString()).toDate());
                }
                if (dbObject.containsField("createdAt")) {
                    dbObject.put("createdAt", DateTime.parse(dbObject.get("createdAt").toString()).toDate());
                }

                sourceDB.save(dbObject);
            }
        } catch (Exception e) {
            fail("Failed to load data to mongo\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    private void loadSOLRData(final String pathToJSONFile) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            for (int retry = 1; retry <= 11; ++retry) {
                if (11 == retry) {
                    fail("Couldn't load data to solr");
                    return;
                }

                final SolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());

                for (final Object object : rootObject) {
                    final JSONObject jsonObject = (JSONObject) object;
                    final SolrInputDocument inputDocument = new SolrInputDocument();

                    inputDocument.addField("europeana_id", jsonObject.get("europeana_id"));

                    try {
                        solrServer.add(inputDocument);
                    } catch (Exception e) {
                        System.out.println("Failed to load document " + jsonObject.toJSONString());
                    }
                }

                try {
                    solrServer.commit();
                    solrServer.shutdown();
                    return;
                } catch (Exception e) {
                    System.out.print("Failed to commit data to solr\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
                    TimeUnit.SECONDS.sleep(10 * retry);
                }
            }
        } catch (Exception e) {
            fail("Failed to load data to solr\n" + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
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
        } catch (ConfigException.Null e) {
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
        } catch (ConfigException.Null e) {
        } catch (FileNotFoundException e) {
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
        publisherConfig = createPublisherConfig(PATH_PREFIX + "config-files/validData/publisher.conf");
        loadMongoData(PATH_PREFIX + "data-files/validData/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(PATH_PREFIX + "data-files/validData/metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData(PATH_PREFIX + "data-files/validData/solrData.json");

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
        ).getDB(publisherConfig.getTargetDBName())
                .getCollection("WebResourceMetaInfo");


        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void testPublishAllDataAfterDate() {
        publisherConfig = createPublisherConfig(PATH_PREFIX + "config-files/filterDataByDate/publisher.conf");
        loadMongoData(PATH_PREFIX + "data-files/filterDataByDate/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(PATH_PREFIX + "data-files/filterDataByDate/metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData(PATH_PREFIX + "data-files/filterDataByDate/solrData.json");

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
        ).getDB(publisherConfig.getTargetDBName())
                .getCollection("WebResourceMetaInfo");

        final BasicDBObject findQuery = new BasicDBObject();
        final String[] validIds = new String[]{"902b31943b4e66f5578539bfa60f2b82",
                "75a47a2953516381bd9d4d1220bdcfc3",
                "46787456ff68f404835064723a4d1cd9",
                "cc6103bee8aa27bfa11b851a2377a015",
                "764fff8f4654274f9cff28280d7e0008",
                "2d74ba5b07344f9587b5c67693fcb3a5"
        };

        findQuery.put("_id", new BasicDBObject("$in", Arrays.asList(validIds)));

        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(findQuery, keys).sort(new BasicDBObject("_id", 1));
        System.out.println(sourceResults.getQuery().toString());
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));
        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void testPublishAllDataWithMetaData() {
        publisherConfig = createPublisherConfig(PATH_PREFIX + "config-files/dataWithMissingMetaInfo/publisher.conf");
        loadMongoData(PATH_PREFIX + "data-files/dataWithMissingMetaInfo/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(PATH_PREFIX + "data-files/dataWithMissingMetaInfo/metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData(PATH_PREFIX + "data-files/dataWithMissingMetaInfo/solrData.json");

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
        ).getDB(publisherConfig.getTargetDBName())
                .getCollection("WebResourceMetaInfo");


        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void testPublishAllDataWithSolrDocEquivelent() {
        publisherConfig = createPublisherConfig(PATH_PREFIX + "config-files/dataWithMissingSolrDoc/publisher.conf");
        loadMongoData(PATH_PREFIX + "data-files/dataWithMissingSolrDoc/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(PATH_PREFIX + "data-files/dataWithMissingSolrDoc/metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData(PATH_PREFIX + "data-files/dataWithMissingSolrDoc/solrData.json");

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
        ).getDB(publisherConfig.getTargetDBName())
                .getCollection("WebResourceMetaInfo");


        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final BasicDBObject filterQuery = new BasicDBObject();
        final String[] validIds = new String[]{"902b31943b4e66f5578539bfa60f2b82",
                "cc6103bee8aa27bfa11b851a2377a015",
                "764fff8f4654274f9cff28280d7e0008",
                "2d74ba5b07344f9587b5c67693fcb3a5"
        };
        filterQuery.put("_id", new BasicDBObject("$in", Arrays.asList(validIds)));

        final DBCursor sourceResults = sourceMetaInfoDB.find(filterQuery, keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(sourceResults.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }


    @Test
    public void testFakeTags() {
        publisherConfig = createPublisherConfig(PATH_PREFIX + "config-files/dataFakeTags/publisher.conf");
        loadMongoData(PATH_PREFIX + "data-files/dataFakeTags/jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(PATH_PREFIX + "data-files/dataFakeTags/metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadSOLRData(PATH_PREFIX + "data-files/dataFakeTags/solrData.json");

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
        ).getDB(publisherConfig.getTargetDBName())
                .getCollection("WebResourceMetaInfo");

        final BasicDBObject keys = new BasicDBObject();
        keys.put("className", 0);
        keys.put("recordId", 0);

        final DBCursor sourceResults = sourceMetaInfoDB.find(new BasicDBObject(), keys).sort(new BasicDBObject("_id", 1));
        final DBCursor targetResults = targetMetaInfoDB.find(new BasicDBObject()).sort(new BasicDBObject("_id", 1));

        assertArrayEquals(sourceResults.toArray().toArray(), targetResults.toArray().toArray());

        final HttpSolrServer solrServer = new HttpSolrServer(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");
        query.addField("facet_tags");
        query.addField("filter_tags");

        try {
            final QueryResponse solrResults = solrServer.query(query);
            assertEquals(sourceResults.size(), solrResults.getResults().size());

            //go through all solr elements and check to see if the fake tags where generated ok
            for (final SolrDocument document : solrResults.getResults()) {
                final String recordId = (String) document.getFieldValue("europeana_id");
                final Collection<Object> facetTags = document.getFieldValues("facet_tags");

                if (null == facetTags) {
                    fail("Facet tags filed cannot be null for this test!");
                    return;
                }

                System.out.println(recordId);
                for (final Object facetTag : facetTags) {
                    final Map.Entry<String, Object> queryParam = getFacetTagValue((Integer)facetTag);

                    if (null != queryParam) {
                        final BasicDBObject findQuery = new BasicDBObject("recordId", recordId);
                        if (queryParam.getKey().equals("imageMetaInfo.fileSize")) {
                            findQuery.put(queryParam.getKey(), new BasicDBObject("$lt", queryParam.getValue()));
                        }
                        else if (queryParam.getKey().equals("audioMetaInfo.duration")) {
                            findQuery.put(queryParam.getKey(), new BasicDBObject("$lt", queryParam.getValue()));
                        }
                        else if (queryParam.getKey().equals("videoMetaInfo.duration")) {
                            findQuery.put(queryParam.getKey(), new BasicDBObject("$lt", queryParam.getValue()));
                        }
                        else {
                            findQuery.put(queryParam.getKey(), queryParam.getValue());
                        }

                        System.out.println(findQuery.toString());
                        assertTrue("Unkown facet tag: " + facetTag + " for recordId " + recordId,
                                    sourceMetaInfoDB.find(findQuery).size() == 1
                                  );
                    }

                }
            }

        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }

    }

    private Map.Entry<String, Object> getFacetTagValue(final Integer facetTag) {
        final String mimeTye = CommonPropertyExtractor.getMimeType(facetTag);
        final MediaTypeEncoding mediaType = CommonPropertyExtractor.getType(facetTag);
        final String mediaTypeName = mediaType.name().toLowerCase() + "MetaInfo";

        if (null != mimeTye && !mimeTye.trim().isEmpty()) {
            return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".mimeType", mimeTye);
        }

        switch (CommonPropertyExtractor.getType(facetTag)) {
            case IMAGE:  {
                final String aspectRatio = ImagePropertyExtractor.getAspectRatio(facetTag).trim();
                final Integer size = ImagePropertyExtractor.getSize(facetTag);
                final String color = ImagePropertyExtractor.getColor(facetTag).trim();
                final String colorSpace = ImagePropertyExtractor.getColorSpace(facetTag).trim();

                if (!aspectRatio.isEmpty())
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".orientation", aspectRatio);
                else if (size != Integer.MIN_VALUE)
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".fileSize", size);
                else if (!color.isEmpty())
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".colorPalette", color);
                else if (!colorSpace.isEmpty())
                    return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".colorSpace", colorSpace);
                else return null;
            }

            case VIDEO:
                final Long duration = VideoPropertyExtractor.getDuration(facetTag);
                if (duration != Long.MIN_VALUE) return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".duration", duration);
                else return null;

            case AUDIO:
                final Long durationSound = SoundPropertyExtractor.getDuration(facetTag);
                if (durationSound != Long.MIN_VALUE) return new AbstractMap.SimpleEntry<String, Object>(mediaTypeName + ".duration", durationSound);
                else return null;

            default:
                return null;
        }
    }
}
