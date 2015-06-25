package utils;

import com.mongodb.*;
import com.mongodb.util.JSON;
import eu.europeana.crfmigration.domain.MigratorConfig;
import org.joda.time.DateTime;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.FileReader;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * Created by salexandru on 02.06.2015.
 */
public class MongoDBUtils {
    private final MigratorConfig migratorConfig;

    public  MongoDBUtils (final MigratorConfig migratorConfig) {
        this.migratorConfig = migratorConfig;
    }

    public Mongo connectToDB(final List<ServerAddress> serverAddressList, final String userName, final String password) {
        final Mongo mongo;

        mongo = new Mongo(serverAddressList);

        if (!migratorConfig.getSourceMongoConfig().getdBUsername().equals("")) {
            final DB sourceDB = mongo.getDB("admin");
            final Boolean auth = sourceDB.authenticate(userName, password.toCharArray());
            if (!auth) {
                fail("Mongo source auth error");
            }
        }

        return mongo;
    }

    public void cleanMongoDatabase() {
        try {
            if (null == migratorConfig) {
                fail("Mongo Configuration cannot be null!");
            }

            final Mongo sourceMongo = connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList(),
                                                  migratorConfig.getSourceMongoConfig().getdBUsername(),
                                                  migratorConfig.getSourceMongoConfig().getdBPassword());

            sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getdBName()).getCollection("record").drop();
            sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getdBName()).getCollection("Aggregation").drop();


            final Mongo targetMongo = connectToDB(migratorConfig.getTargetMongoConfig().getMongoServerAddressList(),
                                                  migratorConfig.getTargetMongoConfig().getdBUsername(),
                                                  migratorConfig.getTargetMongoConfig().getdBPassword()
                                                 );

            targetMongo.getDB(migratorConfig.getTargetMongoConfig().getdBName()).getCollection("ProcessingJob").drop();
            targetMongo.getDB(migratorConfig.getTargetMongoConfig().getdBName()).getCollection("SourceDocumentReference").drop();

        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    public DB connectToSource () {
        return connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList(),
                           migratorConfig.getSourceMongoConfig().getdBUsername(),
                           migratorConfig.getSourceMongoConfig().getdBPassword()
                          ).getDB(migratorConfig.getSourceMongoConfig().getdBName());
    }

    public DB connectToTarget () {
        return connectToDB(migratorConfig.getTargetMongoConfig().getMongoServerAddressList(),
                           migratorConfig.getTargetMongoConfig().getdBUsername(),
                           migratorConfig.getTargetMongoConfig().getdBPassword()
                          ).getDB(migratorConfig.getTargetMongoConfig().getdBName());
    }

    public void loadMongoData(final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList(),
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
}
