package utilities;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.publisher.Publisher;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 09.06.2015.
 */
public class DButils {
    public static  void cleanMongoDatabase(final PublisherConfig config) {
        try {
            if (null == config || null == config.getSourceMongoConfig() || null == config.getTargetDBConfig()) {
                fail("Mongo Configs cannot be null!");
            }

            final DB sourceMongo = config.getSourceMongoConfig().connectToDB();

            sourceMongo.getCollection("SourceDocumentProcessingStatistics").drop();
            sourceMongo.getCollection("SourceDocumentReferenceMetaInfo").drop();

            for (final DBTargetConfig targetConfig: config.getTargetDBConfig()) {

                final DB targetMongo = targetConfig.getMongoConfig().connectToDB();

                targetMongo.getCollection("WebResourceMetaInfo").drop();
                targetMongo.getCollection("WebResource").drop();
            }

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

            final DB sourceMongo = sourceMongoConfig.connectToDB();
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
