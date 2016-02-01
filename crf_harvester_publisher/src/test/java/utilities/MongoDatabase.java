package utilities;

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
import eu.europeana.publisher.domain.PublisherConfig;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.fail;

public class MongoDatabase {

    private MongodProcess mongod = null;
    private MongodExecutable mongodExecutable = null;

    public MongoDatabase(final PublisherConfig publisherConfig) throws IOException {
        MongodStarter starter = MongodStarter.getDefaultInstance();

        IMongodConfig mongodConfig = new MongodConfigBuilder()
                .version(Version.Main.PRODUCTION)
                .net(new Net(27017, Network.localhostIsIPv6()))
                .build();

        mongodExecutable = starter.prepare(mongodConfig);
        mongodExecutable.start();

    }

    public Mongo connectToDB(final String host,final int port) throws IOException {

        return new MongoClient(host, port);
    }

    public void cleanMongoDatabase() {
        mongodExecutable.stop();
    }

    public void loadMongoData(final MongoConfig sourceMongoConfig, final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final DB sourceMongo = connectToDB("localhost",27017).getDB(sourceMongoConfig.getDbName());
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

}
