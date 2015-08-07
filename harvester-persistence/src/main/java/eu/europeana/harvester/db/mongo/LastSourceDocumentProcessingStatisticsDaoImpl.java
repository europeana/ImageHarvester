package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Key;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.LastSourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.*;

import java.util.*;

/**
 * Created by salexandru on 22.07.2015.
 */
public class LastSourceDocumentProcessingStatisticsDaoImpl implements LastSourceDocumentProcessingStatisticsDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public LastSourceDocumentProcessingStatisticsDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public Long getCount() {
        return datastore.getCount(LastSourceDocumentProcessingStatistics.class);
    }

    @Override
    public boolean create(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatisticss, WriteConcern writeConcern) {
        if(read(lastSourceDocumentProcessingStatisticss.getId()) == null) {
            datastore.save(lastSourceDocumentProcessingStatisticss);
            return true;
        } else {
            return false;
        }
    }


    @Override
    public LastSourceDocumentProcessingStatistics read(String id) {
        return datastore.get(LastSourceDocumentProcessingStatistics.class, id);
    }

    @Override
    public LastSourceDocumentProcessingStatistics read (String sourceDocumentReferenceId,
                                                        DocumentReferenceTaskType taskType,
                                                        URLSourceType urlSourceType) {
        return read(LastSourceDocumentProcessingStatistics.idOf(sourceDocumentReferenceId, taskType, urlSourceType));
    }

    @Override
    public boolean update(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatisticss, WriteConcern writeConcern) {
        if(read(lastSourceDocumentProcessingStatisticss.getId()) != null) {
            datastore.save(lastSourceDocumentProcessingStatisticss, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics> createOrModify(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatisticss, WriteConcern writeConcern) {
        return datastore.save(lastSourceDocumentProcessingStatisticss, writeConcern);
    }

    @Override
    public Iterable<com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics>> createOrModify (Collection<LastSourceDocumentProcessingStatistics> lastSourceDocumentProcessingStatisticss,
                                                                                                     WriteConcern writeConcern) {
        if (null == lastSourceDocumentProcessingStatisticss || lastSourceDocumentProcessingStatisticss.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(lastSourceDocumentProcessingStatisticss, writeConcern);
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(LastSourceDocumentProcessingStatistics.class, id);
    }

    @Override
    public List<LastSourceDocumentProcessingStatistics> findByRecordID(String recordID) {
        final Query<LastSourceDocumentProcessingStatistics> query = datastore.find(LastSourceDocumentProcessingStatistics.class, "referenceOwner.recordId", recordID);
        if(query == null) {return new ArrayList<>(0);}

        return query.asList();
    }

    @Override
    public Map<ProcessingState, Long> countNumberOfDocumentsWithState () {
        final DBCollection collection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);

        final BasicDBList matchElements = new BasicDBList();

        matchElements.add(new BasicDBObject("state", ProcessingState.ERROR.name()));
        matchElements.add(new BasicDBObject("state", ProcessingState.SUCCESS.name()));
        matchElements.add(new BasicDBObject("state", ProcessingState.READY.name()));


        final DBObject matchQuery = new BasicDBObject("$match", new BasicDBObject("$or", matchElements));


        final DBObject groupStateElements = new BasicDBObject();

        groupStateElements.put("_id", "$state");
        groupStateElements.put("count", new BasicDBObject("$sum", 1));

        final DBObject groupQuery = new BasicDBObject("$group", groupStateElements);

        final Map<ProcessingState, Long> results = new HashMap<>(ProcessingState.values().length, 1);

        for (final DBObject object: collection.aggregate(matchQuery, groupQuery).results()) {
            long count = ((Number)object.get("count")).longValue();

            results.put(ProcessingState.valueOf((String) object.get("_id")), count);
        }

        return results;
    }

    @Override
    public List<LastSourceDocumentProcessingStatistics> deactivateDocuments (List<String> sourceDocumentReferenceIds, WriteConcern writeConcern) {
        if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        final Query<LastSourceDocumentProcessingStatistics> query = datastore.find(LastSourceDocumentProcessingStatistics.class);

        query.field("sourceDocumentReferenceId").hasAnyOf(sourceDocumentReferenceIds);

        final UpdateOperations<LastSourceDocumentProcessingStatistics> update = datastore.createUpdateOperations(LastSourceDocumentProcessingStatistics.class);

        update.set("active", false);

        datastore.update(query, update, false, writeConcern);

        return query.asList();
    }
}
