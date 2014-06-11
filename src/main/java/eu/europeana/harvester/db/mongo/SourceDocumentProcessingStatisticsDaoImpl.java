package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;

import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with source_document_processing_stats collection
 */
public class SourceDocumentProcessingStatisticsDaoImpl implements SourceDocumentProcessingStatisticsDao {

    private final Datastore datastore;

    public SourceDocumentProcessingStatisticsDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public void create(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics) {
        datastore.save(sourceDocumentProcessingStatistics);
    }

    @Override
    public SourceDocumentProcessingStatistics read(String id) {
        Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class);
        query.criteria("id").equal(id);

        List<SourceDocumentProcessingStatistics> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics) {
        Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class);
        query.criteria("id").equal(sourceDocumentProcessingStatistics.getId());

        List<SourceDocumentProcessingStatistics> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(query);
            datastore.save(sourceDocumentProcessingStatistics);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics) {
        Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class);
        query.criteria("id").equal(sourceDocumentProcessingStatistics.getId());

        List<SourceDocumentProcessingStatistics> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(sourceDocumentProcessingStatistics);

            return true;
        }

        return false;
    }

    @Override
    public SourceDocumentProcessingStatistics findBySourceDocumentReferenceAndJobId(String docId, String jobId) {
        Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class);
        query.criteria("sourceDocumentReferenceId").equal(docId);
        query.criteria("processingJobId").equal(jobId);

        List<SourceDocumentProcessingStatistics> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

}