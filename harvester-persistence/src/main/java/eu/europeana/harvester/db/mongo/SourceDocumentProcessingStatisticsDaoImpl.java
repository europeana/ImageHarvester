package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.google.common.collect.Lists;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

import java.util.*;

/**
 * MongoDB DAO implementation for CRUD with source_document_processing_stats collection
 */
public class SourceDocumentProcessingStatisticsDaoImpl implements SourceDocumentProcessingStatisticsDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public SourceDocumentProcessingStatisticsDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    private final static int THRESHOLD = 1000;

    @Override
    public Long getCount() {
        return datastore.getCount(SourceDocumentProcessingStatistics.class);
    }

    @Override
    public boolean create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern) {
        if (read(sourceDocumentProcessingStatistics.getId()) == null) {
            datastore.save(sourceDocumentProcessingStatistics);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public SourceDocumentProcessingStatistics read(String id) {
        return datastore.get(SourceDocumentProcessingStatistics.class, id);
    }

    @Override
    public List<SourceDocumentProcessingStatistics> read(List<String> ids) {
        if (ids.size() == 0)
            return new ArrayList<>(0);
        else {
            final Query<SourceDocumentProcessingStatistics> query = datastore.createQuery(SourceDocumentProcessingStatistics.class)
                    .field("_id").hasAnyOf(ids)
                    .hintIndex("_id_");
            if (query == null) {
                return new ArrayList<>(0);
            }
            return query.asList();
        }
    }


    @Override
    public boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern) {
        if (read(sourceDocumentProcessingStatistics.getId()) != null) {
            datastore.save(sourceDocumentProcessingStatistics, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public com.google.code.morphia.Key<SourceDocumentProcessingStatistics> createOrModify(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern) {
        return datastore.save(sourceDocumentProcessingStatistics, writeConcern);
    }

    @Override
    public Iterable<com.google.code.morphia.Key<SourceDocumentProcessingStatistics>> createOrModify(Collection<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatistics,
                                                                                                    WriteConcern writeConcern) {
        if (null == sourceDocumentProcessingStatistics || sourceDocumentProcessingStatistics.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(sourceDocumentProcessingStatistics, writeConcern);
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(SourceDocumentProcessingStatistics.class, id);
    }

    @Override
    public SourceDocumentProcessingStatistics findBySourceDocumentReferenceAndJobId(String docId, String jobId) {
        return read(docId + "-" + jobId);
    }

    @Override
    public List<SourceDocumentProcessingStatistics> findByRecordID(String recordID) {
        final Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class, "referenceOwner.recordId", recordID);
        if (query == null) {
            return new ArrayList<>(0);
        }

        return query.asList();
    }

    @Override
    public Map<ProcessingState, Long> countNumberOfDocumentsWithState() {
        final DBCollection collection = datastore.getCollection(SourceDocumentProcessingStatistics.class);

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

        for (final DBObject object : collection.aggregate(matchQuery, groupQuery).results()) {
            long count = ((Number) object.get("count")).longValue();

            results.put(ProcessingState.valueOf((String) object.get("_id")), count);
        }

        return results;
    }

    @Override
    public List<SourceDocumentProcessingStatistics> deactivateDocuments(List<String> sourceDocumentReferenceIds, WriteConcern writeConcern) {
        if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        List<SourceDocumentProcessingStatistics> docs = new ArrayList<>();
        List<List<String>> split = split(sourceDocumentReferenceIds);
        for (List<String> splitted : split) {
            final Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class);

            query.field("sourceDocumentReferenceId").hasAnyOf(splitted);

            final UpdateOperations<SourceDocumentProcessingStatistics> update = datastore.createUpdateOperations(SourceDocumentProcessingStatistics.class);

            update.set("active", false);

            datastore.update(query, update, false, writeConcern);
            //docs.addAll(query.asList());
        }
        return docs;
    }

    private List<List<String>> split(List<String> sourceDocumentReferenceIds) {
        return Lists.partition(sourceDocumentReferenceIds, THRESHOLD);
    }
}
