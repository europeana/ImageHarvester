package utils;

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
import eu.europeana.crfmigration.domain.MigratorConfig;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MongoDBUtils {
    private final MigratorConfig migratorConfig;

    private MongodProcess mongod = null;
    private MongodExecutable mongodExecutable = null;

    public MongoDBUtils(final MigratorConfig migratorConfig) throws IOException {
        this.migratorConfig = migratorConfig;
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

    public DB connectToSource () throws IOException {
        return connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList().get(0).getHost(),migratorConfig.getSourceMongoConfig().getMongoServerAddressList().get(0).getPort()
                          ).getDB(migratorConfig.getSourceMongoConfig().getDbName());
    }

    public DB connectToTarget () throws IOException {
        return connectToDB(migratorConfig.getTargetMongoConfig().getMongoServerAddressList().get(0).getHost(),migratorConfig.getTargetMongoConfig().getMongoServerAddressList().get(0).getPort()).getDB(migratorConfig.getTargetMongoConfig().getDbName());
    }

    public void loadMongoData(final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList().get(0).getHost(),migratorConfig.getSourceMongoConfig().getMongoServerAddressList().get(0).getPort());
            final DBCollection sourceDB = sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getDbName()).getCollection(collectionName);
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
}
