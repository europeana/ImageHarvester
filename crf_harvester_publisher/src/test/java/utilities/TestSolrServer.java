package utilities;

import com.google.common.io.Files;
import com.mongodb.*;
import com.mongodb.util.JSON;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.publisher.Publisher;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.PublisherConfig;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 09.06.2015.
 */
public class TestSolrServer {

    private EmbeddedSolrServer server;

    public TestSolrServer(final DBTargetConfig solrConfig,final String solrXmlPath) throws IOException {
        final String destFolder = "/tmp/solr";
        if (!(new File(destFolder)).exists()) {
            (new File(destFolder)).mkdir();
        }

        final String destPath = "/tmp/solr/solr.xml";

        if ((new File(destPath)).exists()) {
            (new File(destPath)).delete();
        }
        (new File(destPath)).createNewFile();
        System.out.println(solrXmlPath);
        Files.copy(new File(solrXmlPath),new File(destPath));
        server = new EmbeddedSolrServer(Paths.get("/tmp/solr"),"core-test");
    }

    public void shutDown(){
        server.shutdown();
    }

    public  void cleanSolrDatabase(final String solrURL) {
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



    public  void loadSOLRData(final String pathToJSONFile, final String solrUrl) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            for (int retry = 1; retry <= 11; ++retry) {
                if (11 == retry) {
                    fail("Couldn't load data to solr");
                    return;
                }

                final SolrClient client = new HttpSolrClient(solrUrl);

                for (final Object object : rootObject) {
                    final JSONObject jsonObject = (JSONObject) object;
                    final SolrInputDocument inputDocument = new SolrInputDocument();

                    for (final Object key: jsonObject.keySet()) {
                       inputDocument.addField(key.toString(), jsonObject.get(key));
                    }

                    try {
                        client.add(inputDocument);
                    } catch (Exception e) {
                        e.printStackTrace();
                        //System.out.println("Failed to load document " + jsonObject.toJSONString());
                    }
                }

                try {
                    client.commit();
                    client.shutdown();
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
