package europeana.logic;

import com.mongodb.*;
import crf_migration.europeana.domain.MigrationMongoConfig;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class Migrator {

    private final DB db;

    private final Map<String, BasicDBObject> records = new HashMap<String, BasicDBObject>();
    private final Map<String, BasicDBObject> aggregations = new HashMap<String, BasicDBObject>();
    private final Map<String, BasicDBObject> webResources = new HashMap<String, BasicDBObject>();

    public Migrator(MigrationMongoConfig config) throws UnknownHostException {
        final Mongo mongo = new Mongo(config.getHost(), config.getPort());
        db = mongo.getDB(config.getDbName());
    }

    public void migrate() {
        getRecords();
        //getAggregations();
        getWebresources();

        System.out.println("Done");
    }

    private void getRecords() {
        final DBCollection recordCollection = db.getCollection("record");

        final BasicDBObject allQuery = new BasicDBObject();
        final BasicDBObject recordFields = new BasicDBObject();
        recordFields.put("about", 1);
        recordFields.put("europeanaCollectionName", 1);
        recordFields.put("_id", 0);

        final DBCursor recordCursor = recordCollection.find(allQuery, recordFields);
        while (recordCursor.hasNext()) {
            final BasicDBObject item = (BasicDBObject) recordCursor.next();
            final String about = (String) item.get("about");

            records.put(about, item);
        }
    }

    private void getAggregations() {
        final DBCollection aggregationCollection = db.getCollection("Aggregation");

        final BasicDBObject allQuery = new BasicDBObject();
        final BasicDBObject recordFields = new BasicDBObject();
        recordFields.put("about", 1);
        recordFields.put("webResources", 1);
        recordFields.put("_id", 0);

        final DBCursor aggregationCursor = aggregationCollection.find(allQuery, recordFields);
        while (aggregationCursor.hasNext()) {
            final BasicDBObject item = (BasicDBObject) aggregationCursor.next();
            final String about = (String) item.get("about");

            aggregations.put(about, item);
        }
    }

    private void getWebresources() {
        final DBCollection webResourceCollection = db.getCollection("WebResource");

        final BasicDBObject allQuery = new BasicDBObject();
        final BasicDBObject recordFields = new BasicDBObject();
        recordFields.put("about", 1);

        final DBCursor webResourceCursor = webResourceCollection.find(allQuery, recordFields);
        while (webResourceCursor.hasNext()) {
            final BasicDBObject item = (BasicDBObject) webResourceCursor.next();
            final String id = item.get("_id").toString();

            webResources.put(id, item);
        }
    }
}
