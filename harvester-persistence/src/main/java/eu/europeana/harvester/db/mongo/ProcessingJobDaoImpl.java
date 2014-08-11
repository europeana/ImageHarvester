package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.Page;
import eu.europeana.harvester.domain.ProcessingJob;

import java.util.List;

/**
 * MongoDB DAO implementation for CRUD with processing_job collection
 */
public class ProcessingJobDaoImpl implements ProcessingJobDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
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
        return datastore.get(ProcessingJob.class, id);
    }

    @Override
    public boolean update(ProcessingJob processingJob, WriteConcern writeConcern) {
        if(read(processingJob.getId()) != null) {
            datastore.save(processingJob, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(ProcessingJob.class, id);
    }

    @Override
    public List<ProcessingJob> getJobsWithState(JobState jobState, Page page) {
        final Query<ProcessingJob> query = datastore.find(ProcessingJob.class);
        query.criteria("state").equal(jobState);
        query.offset(page.getFrom());
        query.limit(page.getLimit());

        return query.asList();
    }
}
