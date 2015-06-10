package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.mongodb.*;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.Page;
import eu.europeana.harvester.domain.ProcessingJob;

import java.util.*;

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
    public com.google.code.morphia.Key<ProcessingJob> createOrModify(ProcessingJob processingJobs, WriteConcern writeConcern) {
        return datastore.save(processingJobs, writeConcern);
    }

    @Override
    public Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(Collection<ProcessingJob> processingJobs, WriteConcern writeConcern) {
        if (null == processingJobs || processingJobs.isEmpty()) {
            return Collections.EMPTY_LIST;
        }
        return datastore.save(processingJobs, writeConcern);
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

    public Map<String, Integer> getIpDistribution() {
        final DB db = datastore.getDB();
        final DBCollection processingJobCollection = db.getCollection("ProcessingJob");

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
    public List<ProcessingJob> getDiffusedJobsWithState(JobState jobState, Page page, Map<String, Integer> ipDistribution, Map<String, Boolean> ipsWithJobs) {
        final List<ProcessingJob> processingJobs = new ArrayList<>();

        for(Map.Entry<String, Integer> ip: ipDistribution.entrySet()) {
            final Query<ProcessingJob> query = datastore.find(ProcessingJob.class);
            query.criteria("state").equal(jobState);
            query.criteria("ipAddress").equal(ip.getKey());
            query.limit(page.getLimit());
            final List<ProcessingJob> temp = query.asList();

            if(temp.size() == 0) {
                ipsWithJobs.put(ip.getKey(), false);
            } else {
                ipsWithJobs.put(ip.getKey(), true);
            }

            processingJobs.addAll(temp);
        }

        return processingJobs;
    }

}
