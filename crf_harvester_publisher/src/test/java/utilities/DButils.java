package utilities;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;
import eu.europeana.publisher.domain.MongoConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 09.06.2015.
 */
public class DButils {
    public static DB connectToDB (final MongoConfig mongoConfig) {
        final Mongo mongo;
        final String host = mongoConfig.getHost();
        final Integer port =mongoConfig.getPort();
        final String userName = mongoConfig.getdBUsername();
        final String password = mongoConfig.getdBPassword();
        final String dbName   = mongoConfig.getdBName();

        try {
            mongo = new Mongo(host, port);
        } catch (UnknownHostException e) {
            fail("Cannot connect to mongo database. Unkown host: " + e.getMessage());
            return null;
        }


        if (StringUtils.isNotEmpty(userName)) {
            final DB sourceDB = mongo.getDB("admin");
            final Boolean auth = sourceDB.authenticate(userName, password.toCharArray());
            if (!auth) {
                fail("Mongo source auth error");
            }
        }

        return mongo.getDB(dbName);
    }


    public static  void cleanMongoDatabase(final MongoConfig sourceConfig, final MongoConfig targetConfig) {
        try {
            if (null == sourceConfig || null == targetConfig) {
                fail("Mongo Configs cannot be null!");
            }

            final DB sourceMongo = connectToDB(sourceConfig);

            sourceMongo.getCollection("SourceDocumentProcessingStatistics").drop();
            sourceMongo.getCollection("SourceDocumentReferenceMetaInfo").drop();


            final DB targetMongo = connectToDB(targetConfig);

            targetMongo.getCollection("WebResourceMetaInfo").drop();
            targetMongo.getCollection("WebResource").drop();

        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    public static void cleanSolrDatabase(final String solrURL) {
        try {
            if (null == solrURL) {
                fail("url to solr cannot be null");
            }

            final SolrClient solrServer = new HttpSolrClient(solrURL);
            solrServer.deleteByQuery("*:*");
            solrServer.commit();
        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }


    public static void loadMongoData(final MongoConfig sourceMongoConfig, final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final DB sourceMongo = connectToDB(sourceMongoConfig);
            final DBCollection sourceDB = sourceMongo.getCollection(collectionName);
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

    public static void loadSOLRData(final String pathToJSONFile, final String solrUrl) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            for (int retry = 1; retry <= 11; ++retry) {
                if (11 == retry) {
                    fail("Couldn't load data to solr");
                    return;
                }

                final SolrClient solrServer = new HttpSolrClient(solrUrl);

                for (final Object object : rootObject) {
                    final JSONObject jsonObject = (JSONObject) object;
                    final SolrInputDocument inputDocument = new SolrInputDocument();

                    for (final Object key: jsonObject.keySet()) {
                       inputDocument.addField(key.toString(), jsonObject.get(key));
                    }

                    try {
                        solrServer.add(inputDocument);
                    } catch (Exception e) {
                        e.printStackTrace();
                        //System.out.println("Failed to load document " + jsonObject.toJSONString());
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
}
