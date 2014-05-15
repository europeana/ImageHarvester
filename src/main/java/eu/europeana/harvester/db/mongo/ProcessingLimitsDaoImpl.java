package eu.europeana.harvester.db.mongo;

import eu.europeana.harvester.db.ProcessingLimitsDao;
import eu.europeana.harvester.domain.ProcessingLimits;
import org.mongodb.morphia.Datastore;

import org.mongodb.morphia.query.Query;

import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with processing_limits collection
 */
public class ProcessingLimitsDaoImpl implements ProcessingLimitsDao {

    private final Datastore datastore;

    public ProcessingLimitsDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public void create(ProcessingLimits processingLimits) {
        datastore.save(processingLimits);
    }

    @Override
    public ProcessingLimits read(String id) {
        Query<ProcessingLimits> query = datastore.find(ProcessingLimits.class);
        query.criteria("id").equal(id);

        List<ProcessingLimits> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(ProcessingLimits processingLimits) {
        Query<ProcessingLimits> query = datastore.find(ProcessingLimits.class);
        query.criteria("id").equal(processingLimits.getId());

        List<ProcessingLimits> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(query);
            datastore.save(processingLimits);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(ProcessingLimits processingLimits) {
        Query<ProcessingLimits> query = datastore.find(ProcessingLimits.class);
        query.criteria("id").equal(processingLimits.getId());

        List<ProcessingLimits> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(processingLimits);

            return true;
        }

        return false;
    }

   @Override
    public void createOrModify(ProcessingLimits processingLimits) {
        delete(processingLimits);
        create(processingLimits);
    }

    @Override
    public ProcessingLimits findByCollectionId(Long id) {
        Query<ProcessingLimits> query = datastore.find(ProcessingLimits.class);
        query.criteria("collectionId").equal(id);

        List<ProcessingLimits> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

}
