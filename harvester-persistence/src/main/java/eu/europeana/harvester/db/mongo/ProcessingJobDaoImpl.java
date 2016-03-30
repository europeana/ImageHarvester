package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.*;

import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.util.pagedElements.PagedElements;
import eu.europeana.harvester.util.pagedElements.PagedProcessingJobElements;

import java.util.*;

/**
 * MongoDB DAO implementation for CRUD with processing_job collection
 */
public class ProcessingJobDaoImpl implements ProcessingJobDao {

	/**
	 * The Datastore interface provides type-safe methods for accessing and
	 * storing your java objects in MongoDB. It provides get/find/save/delete
	 * methods for working with your java objects.
	 */
	private final Datastore datastore;

	public ProcessingJobDaoImpl(Datastore datastore) {
		this.datastore = datastore;
	}

	@Override
	public Long getCount() {
		return datastore.getCount(ProcessingJob.class);
	}

	@Override
	public boolean create(ProcessingJob processingJob, WriteConcern writeConcern) {
		if (read(processingJob.getId()) == null) {
			datastore.save(processingJob, writeConcern);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public com.google.code.morphia.Key<ProcessingJob> createOrModify(
			ProcessingJob processingJobs, WriteConcern writeConcern) {
		return datastore.save(processingJobs, writeConcern);
	}

	@Override
	public Iterable<com.google.code.morphia.Key<ProcessingJob>> createOrModify(
			Collection<ProcessingJob> processingJobs, WriteConcern writeConcern) {
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
		if (read(processingJob.getId()) != null) {
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

	@Override
	public void modifyStateOfJobsWithIds(JobState newJobState,
			final List<String> jobIds) {
		if (jobIds.isEmpty())
			return;
		final Query<ProcessingJob> query = datastore
				.createQuery(ProcessingJob.class).field("_id")
				.in(new ArrayList<>(jobIds));
		final UpdateOperations<ProcessingJob> ops = datastore
				.createUpdateOperations(ProcessingJob.class).set("state",
						newJobState);
		datastore.update(query, ops);
	}

	@Override
	public void modifyStateOfJobs(JobState oldJobState, JobState newJobState) {
		final Query<ProcessingJob> query = datastore
				.createQuery(ProcessingJob.class).field("state")
				.equal(oldJobState);
		final UpdateOperations<ProcessingJob> ops = datastore
				.createUpdateOperations(ProcessingJob.class).set("state",
						newJobState);
		datastore.update(query, ops);
	}

	public Map<String, Integer> getIpDistribution() {
		final DB db = datastore.getDB();
		final DBCollection processingJobCollection = db
				.getCollection("ProcessingJob");

		final DBObject match = new BasicDBObject();
		match.put("state", "READY");

		final DBObject group = new BasicDBObject();
		group.put("_id", "$ipAddress");
		group.put("total", new BasicDBObject("$sum", 1));

		final AggregationOutput output = processingJobCollection.aggregate(
				new BasicDBObject("$match", match), new BasicDBObject("$group",
						group));
		final Map<String, Integer> jobsPerIP = new HashMap<>();

		if (output != null) {
			for (DBObject result : output.results()) {
				final String ip = (String) result.get("_id");
				final Integer count = (Integer) result.get("total");
				if (ip != null)
					jobsPerIP.put(ip, count);
			}
		}

		return jobsPerIP;
	}

	@Override
	public List<ProcessingJob> getDiffusedJobsWithState(
			JobPriority jobPriority, JobState jobState, Page page,
			Map<String, Integer> ipDistribution) {

		if (ipDistribution.size() <= 0)
			return Collections.EMPTY_LIST;

		final List<ProcessingJob> processingJobs = new ArrayList<>();

		for (final String ip : ipDistribution.keySet()) {
			final Query<ProcessingJob> query = datastore
					.find(ProcessingJob.class);
			query.criteria("priority").equal(jobPriority.getPriority());
			query.criteria("state").equal(jobState);
			query.criteria("ipAddress").equal(ip);
			query.limit(page.getLimit());
			processingJobs.addAll(query.asList());
		}
		return processingJobs;
	}

	@Override
	public List<ProcessingJob> deactivateJobs(final ReferenceOwner owner,
			final WriteConcern writeConcern) {
		if (null == owner || (owner.equals(new ReferenceOwner()))) {
			throw new IllegalArgumentException(
					"The reference owner cannot be null and must have at least one field not null");
		}

		final Query<ProcessingJob> query = datastore
				.createQuery(ProcessingJob.class);

		if (null != owner.getCollectionId()) {
			query.criteria("referenceOwner.collectionId").equal(
					owner.getCollectionId());
		}

		if (null != owner.getRecordId()) {
			query.criteria("referenceOwner.recordId")
					.equal(owner.getRecordId());
		}

		if (null != owner.getProviderId()) {
			query.criteria("referenceOwner.providerId").equal(
					owner.getProviderId());
		}

		if (null != owner.getExecutionId()) {
			query.criteria("referenceOwner.executionId").equal(
					owner.getExecutionId());
		}

		final UpdateOperations<ProcessingJob> updateOperations = datastore
				.createUpdateOperations(ProcessingJob.class);

		updateOperations.set("active", false);

		datastore.update(query, updateOperations, false, writeConcern);

		return query.asList();
	}

	@Override
	public PagedElements<ProcessingJob> findJobsByCollectionIdAndState(
			final Set<String> collectionIds, final Set<JobState> states,
			final Page pageConfiguration) {
		final BasicDBObject collectionIdFilter = new BasicDBObject();
		collectionIdFilter.put("referenceOwner.collectionId",
				new BasicDBObject("$in", collectionIds));

		final BasicDBObject jobStatesFilter = new BasicDBObject();
		final Set<String> statesAsString = new HashSet<>();
		for (final JobState s : states)
			statesAsString.add(s.name());
		jobStatesFilter.put("state", new BasicDBObject("$in", statesAsString));

		final BasicDBObject query = new BasicDBObject();
		query.put("$and", Arrays.asList(collectionIdFilter, jobStatesFilter));

		final DBCursor cursor = datastore.getCollection(ProcessingJob.class)
				.find(query).skip(pageConfiguration.getFrom())
				.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
		// .addOption(Bytes.QUERYOPTION_SLAVEOK);

		return new PagedProcessingJobElements(cursor,
				pageConfiguration.getLimit());
	}

	@Override
	public Map<String, JobStatistics> findJobsByCollectionId(String collectionId) {
		final DBCollection processingJobCollection = datastore
				.getCollection(ProcessingJob.class);
		final BasicDBObject collIdQuery = new BasicDBObject();
		collIdQuery.put("referenceOwner.collectionId", collectionId);
		List<String> executionIds = processingJobCollection.distinct(
				"referenceOwner.executionId", collIdQuery);
		Map<String, JobStatistics> statistics = new HashMap<>();
		for (String executionId : executionIds) {
			final DBObject match = new BasicDBObject();
			match.put("referenceOwner.executionId", executionId);
			final DBObject group = new BasicDBObject();
			group.put("_id", "$state");
			group.put("total", new BasicDBObject("$sum", 1));
			JobStatistics executionIdStats = new JobStatistics();
			final AggregationOutput output = processingJobCollection.aggregate(
					new BasicDBObject("$match", match), new BasicDBObject(
							"$group", group));
			if (output != null) {

				for (DBObject result : output.results()) {
					final String state = (String) result.get("_id");
					final Integer count = (Integer) result.get("total");
					switch (state) {
					case "FINISHED":
						executionIdStats.setSuccessful(count);
						break;
					case "ERROR":
						executionIdStats.setFailed(executionIdStats.getFailed()
								+ count);
						break;
					case "FAILED":
						executionIdStats.setFailed(executionIdStats.getFailed()
								+ count);
						break;
					default:
						executionIdStats.setPending(executionIdStats
								.getPending() + count);
						break;
					}

				}
			}
			statistics.put(executionId, executionIdStats);
		}
		return statistics;
	}
}
