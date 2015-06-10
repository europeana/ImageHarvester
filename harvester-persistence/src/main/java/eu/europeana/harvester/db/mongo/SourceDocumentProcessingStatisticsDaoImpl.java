package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

    @Override
    public boolean create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern) {
        if(read(sourceDocumentProcessingStatistics.getId()) == null) {
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
    public boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics, WriteConcern writeConcern) {
        if(read(sourceDocumentProcessingStatistics.getId()) != null) {
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
    public Iterable<com.google.code.morphia.Key<SourceDocumentProcessingStatistics>> createOrUpdate(Collection<SourceDocumentProcessingStatistics> sourceDocumentProcessingStatistics, WriteConcern writeConcern) {
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
        if(query == null) {return new ArrayList<>(0);}

        return query.asList();
    }

}
