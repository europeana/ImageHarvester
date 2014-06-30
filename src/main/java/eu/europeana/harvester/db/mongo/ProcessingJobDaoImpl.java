package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.domain.ProcessingJob;

import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with processing_job collection
 */
public class ProcessingJobDaoImpl  implements ProcessingJobDao {

    private final Datastore datastore;

    public ProcessingJobDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(ProcessingJob processingJob, WriteConcern writeConcern) {
        if(read(processingJob.getId()) == null) {
            datastore.save(processingJob, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ProcessingJob read(String id) {
        Query<ProcessingJob> query = datastore.find(ProcessingJob.class);
        query.criteria("id").equal(id);

        List<ProcessingJob> result = query.asList();
        if(!result.isEmpty()) {
            return result.get(0);
        }
        return null;
    }

    @Override
    public boolean update(ProcessingJob processingJob, WriteConcern writeConcern) {
        Query<ProcessingJob> query = datastore.find(ProcessingJob.class);
        query.criteria("id").equal(processingJob.getId());

        List<ProcessingJob> result = query.asList();
        if(!result.isEmpty()) {
            datastore.save(processingJob, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public boolean delete(ProcessingJob processingJob, WriteConcern writeConcern) {
        Query<ProcessingJob> query = datastore.find(ProcessingJob.class);
        query.criteria("id").equal(processingJob.getId());

        List<ProcessingJob> result = query.asList();
        if(!result.isEmpty()) {
            datastore.delete(processingJob, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public List<ProcessingJob> getAllJobs() {
        Query<ProcessingJob> query = datastore.find(ProcessingJob.class);

        return query.asList();
    }
}
