package eu.europeana.crfmigration.dao;

import com.mongodb.*;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.crfmigration.logic.MigratorMetrics;
import eu.europeana.harvester.domain.ReferenceOwner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.shared.utils.StringUtils;

import java.net.UnknownHostException;
import java.util.*;

public class MigratorEuropeanaDao {
    private static final Logger LOG = LogManager.getLogger(MigratorEuropeanaDao.class.getName());

    private final MongoConfig mongoConfig;
    private final Mongo mongo;
    private final DB database;
    private final MigratorMetrics metrics;

    public MigratorEuropeanaDao(MongoConfig mongoConfig, final MigratorMetrics metrics) throws UnknownHostException {
        this.mongoConfig = mongoConfig;

        mongo = new Mongo(mongoConfig.getHost(), mongoConfig.getPort());

        if (StringUtils.isNotEmpty(mongoConfig.getdBUsername()) && StringUtils.isNotEmpty(mongoConfig.getdBPassword())) {
            final DB authDB = mongo.getDB("admin");
            final Boolean auth = authDB.authenticate(mongoConfig.getdBUsername(),
                    mongoConfig.getdBPassword().toCharArray());
            if (!auth) {
                throw new MongoException("Cannot authenticate to mongo database");
            }
        }
        database = mongo.getDB(mongoConfig.getdBName());
        this.metrics = metrics;
    }

    public final Map<String, String> retrieveRecordsIdsFromCursor(final DBCursor recordCursor,int batchSize) {
        final Map<String, String> records = new HashMap<>();
        int i = 0;
        while (recordCursor.hasNext() || (i >= batchSize)) {
            final BasicDBObject item = (BasicDBObject) recordCursor.next();
            final String about = (String) item.get("about");
            final BasicDBList collNames = (BasicDBList) item.get("europeanaCollectionName");
            final String collectionId = (String) collNames.get(0);
            records.put(about, collectionId);
            i = i + 1;
        }
        return records;
    }

    public final DBCursor buildRecordsRetrievalCursorByFilter(Date moreRecentThan) {
        final DBCollection recordCollection = database.getCollection("record");
        DBObject filterByTimestampQuery = new BasicDBObject();
        final BasicDBObject recordFields = new BasicDBObject();

        if (null != moreRecentThan) {
            filterByTimestampQuery = QueryBuilder.start().put("timestampUpdated").greaterThanEquals(moreRecentThan).get();
            LOG.info("Query: " + filterByTimestampQuery);
        }

        recordFields.put("about", 1);
        recordFields.put("", 1);
        recordFields.put("timestampUpdated", 1);
        recordFields.put("europeanaCollectionName", 1);
        recordFields.put("_id", 0);
        final BasicDBObject sortOrder = new BasicDBObject();
        sortOrder.put("$natural", 1);

        final DBCursor recordCursor = recordCollection.find(filterByTimestampQuery, recordFields).sort(sortOrder);
        recordCursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
        return recordCursor;
    }

    public final List<EuropeanaEDMObject> retrieveSourceDocumentReferences(final Map<String, String> records) {
        final List<EuropeanaEDMObject> results = new ArrayList<>();
        for (final Map.Entry<String, String> record : records.entrySet()) {
            final ReferenceOwner referenceOwner = getReferenceOwner(record);

            final DBObject aggregation = getAggregation("/aggregation/provider" + record.getKey());

            if (null == aggregation) {
                LOG.error("Missing aggregation: /aggregation/provider" + record.getKey() + " at record: " + record.getKey() + "\n");
                metrics.incEmptyAggregations();
                continue;
            }

            final String edmObject = (String) aggregation.get("edmObject");
            final BasicDBList hasViews = (BasicDBList) aggregation.get("hasView");
            final String edmIsShownBy = (String) aggregation.get("edmIsShownBy");
            final String edmIsShownAt = (String) aggregation.get("edmIsShownAt");


            final List<String> edmHasViews = null == hasViews ?
                    null :
                    Arrays.asList(hasViews.toArray(new String[hasViews.size()]));

            results.add(new EuropeanaEDMObject(referenceOwner, edmObject, edmIsShownBy, edmIsShownAt, edmHasViews));
        }
        return results;
    }

    private final DBObject getAggregation(String aggregationAbout) {
        try {
            metrics.startGetAggregationTimer();

            final DBCollection aggregationCollection = database.getCollection("Aggregation");

            final BasicDBObject whereQueryAggregation = new BasicDBObject();
            whereQueryAggregation.put("about", aggregationAbout);

            final BasicDBObject aggregationFields = new BasicDBObject();
            aggregationFields.put("edmObject", 1);
            aggregationFields.put("edmIsShownBy", 1);
            aggregationFields.put("edmIsShownAt", 1);
            aggregationFields.put("hasView", 1);
            aggregationFields.put("_id", 0);

            return aggregationCollection.findOne(whereQueryAggregation, aggregationFields);
        } finally {
            metrics.stopGetAggregationTimer();
        }
    }

    private final ReferenceOwner getReferenceOwner(final Map.Entry pairs) {
        final String about = (String) pairs.getKey();
        final String collectionId = (String) pairs.getValue();

        final String[] temp = about.split("/");
        final String providerId = temp[1];
        final String recordId = about;

        return new ReferenceOwner(providerId, collectionId, recordId);
    }


}
