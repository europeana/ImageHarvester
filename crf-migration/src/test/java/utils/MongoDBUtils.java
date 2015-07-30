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

        if (!migratorConfig.getSourceMongoConfig().getUsername().equals("")) {
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
                                                  migratorConfig.getSourceMongoConfig().getUsername(),
                                                  migratorConfig.getSourceMongoConfig().getPassword());

            for (String name: sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getDbName()).getCollectionNames()) {
                if ("system.indexes".equalsIgnoreCase(name)) continue;
                if ("system.users".equalsIgnoreCase(name)) continue;
                sourceMongo.getDB(migratorConfig.getSourceMongoConfig().getDbName()).getCollection(name).drop();
            }

            final Mongo targetMongo = connectToDB(migratorConfig.getTargetMongoConfig().getMongoServerAddressList(),
                                                  migratorConfig.getTargetMongoConfig().getUsername(),
                                                  migratorConfig.getTargetMongoConfig().getPassword()
                                                 );

            for (String name: targetMongo.getDB(migratorConfig.getTargetMongoConfig().getDbName()).getCollectionNames()) {
                if ("system.indexes".equalsIgnoreCase(name)) continue;
                if ("system.users".equalsIgnoreCase(name)) continue;
                targetMongo.getDB(migratorConfig.getTargetMongoConfig().getDbName())
                           .getCollection(name).drop();
            }



        } catch (Exception e) {
            fail("Clean Mongo Database has failed with Exception: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    public DB connectToSource () {
        return connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList(),
                           migratorConfig.getSourceMongoConfig().getUsername(),
                           migratorConfig.getSourceMongoConfig().getPassword()
                          ).getDB(migratorConfig.getSourceMongoConfig().getDbName());
    }

    public DB connectToTarget () {
        return connectToDB(migratorConfig.getTargetMongoConfig().getMongoServerAddressList(),
                           migratorConfig.getTargetMongoConfig().getUsername(),
                           migratorConfig.getTargetMongoConfig().getPassword()
                          ).getDB(migratorConfig.getTargetMongoConfig().getDbName());
    }

    public void loadMongoData(final String pathToJSONFile, final String collectionName) {
        try {
            JSONParser parser = new JSONParser();
            JSONArray rootObject = (JSONArray) parser.parse(new FileReader(pathToJSONFile));

            final Mongo sourceMongo = connectToDB(migratorConfig.getSourceMongoConfig().getMongoServerAddressList(),
                                                  migratorConfig.getSourceMongoConfig().getUsername(),
                                                  migratorConfig.getSourceMongoConfig().getPassword());
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
