package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.HistoricalProcessingJobDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.domain.*;

import java.util.*;

/**
 * MongoDB DAO implementation for CRUD with processing_job collection
 */
public class HistoricalProcessingJobDaoImpl implements HistoricalProcessingJobDao {

    /**
     * The Datastore interface provides type-safe methods for accessing and storing your java objects in MongoDB.
     * It provides get/find/save/delete methods for working with your java objects.
     */
    private final Datastore datastore;

    public HistoricalProcessingJobDaoImpl(Datastore datastore) {
        this.datastore = datastore;
    }

    @Override
    public boolean create(HistoricalProcessingJob historicalProcessingJob, WriteConcern writeConcern) {
        if(read(historicalProcessingJob.getId()) == null) {
            datastore.save(historicalProcessingJob, writeConcern);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public com.google.code.morphia.Key<HistoricalProcessingJob> createOrModify(HistoricalProcessingJob historicalProcessingJob, WriteConcern writeConcern) {
        return datastore.save(historicalProcessingJob, writeConcern);
    }

    @Override
    public Iterable<com.google.code.morphia.Key<HistoricalProcessingJob>> createOrModify(Collection<HistoricalProcessingJob> historicalProcessingJobs, WriteConcern writeConcern) {
        if (null == historicalProcessingJobs || historicalProcessingJobs.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(historicalProcessingJobs, writeConcern);
    }

    @Override
    public HistoricalProcessingJob read(String id) {
        return datastore.get(HistoricalProcessingJob.class, id);
    }

    @Override
    public boolean update(HistoricalProcessingJob historicalProcessingJob, WriteConcern writeConcern) {
        if(read(historicalProcessingJob.getId()) != null) {
            datastore.save(historicalProcessingJob, writeConcern);

            return true;
        }

        return false;
    }

    @Override
    public WriteResult delete(String id) {
        return datastore.delete(HistoricalProcessingJob.class, id);
    }

    @Override
    public List<HistoricalProcessingJob> getJobsWithState(JobState jobState, Page page) {
        final Query<HistoricalProcessingJob> query = datastore.find(HistoricalProcessingJob.class);
        query.criteria("state").equal(jobState);
        query.offset(page.getFrom());
        query.limit(page.getLimit());

        return query.asList();
    }

    public Map<String, Integer> getIpDistribution() {
        final DB db = datastore.getDB();
        final DBCollection processingJobCollection = db.getCollection("HistoricalProcessingJob");

        final DBObject match = new BasicDBObject();
        match.put("state", "READY");

        final DBObject group = new BasicDBObject();
        group.put("_id", "$ipAddress");
        group.put("total", new BasicDBObject("$sum", 1));

        final AggregationOutput output = processingJobCollection.aggregate(new BasicDBObject("$match", match), new BasicDBObject("$group", group));
        final Map<String, Integer> jobsPerIP = new HashMap<>();

        if (output != null) {
            for (DBObject result : output.results()) {
                final String ip = (String) result.get("_id");
                final Integer count = (Integer) result.get("total");
                if(ip!=null)
                    jobsPerIP.put(ip, count);
            }
        }

        return jobsPerIP;
    }


    @Override
    public List<HistoricalProcessingJob> deactivateJobs (final ReferenceOwner owner, final WriteConcern writeConcern) {
        if (null == owner || (owner.equals(new ReferenceOwner()))) {
            throw new IllegalArgumentException("The reference owner cannot be null and must have at least one field not null");
        }

        final Query<HistoricalProcessingJob> query = datastore.createQuery(HistoricalProcessingJob.class);

        if (null != owner.getCollectionId()) {
            query.criteria("referenceOwner.collectionId").equal(owner.getCollectionId());
        }

        if (null != owner.getRecordId()) {
            query.criteria("referenceOwner.recordId").equal(owner.getRecordId());
        }

        if (null != owner.getProviderId()) {
            query.criteria("referenceOwner.providerId").equal(owner.getProviderId());
        }

        if (null != owner.getExecutionId()) {
            query.criteria("referenceOwner.executionId").equal(owner.getExecutionId());
        }

        final UpdateOperations<HistoricalProcessingJob> updateOperations = datastore.createUpdateOperations(HistoricalProcessingJob.class);

        updateOperations.set("active", false);

        datastore.update(query, updateOperations, false, writeConcern);

        return query.asList();
    }




}
