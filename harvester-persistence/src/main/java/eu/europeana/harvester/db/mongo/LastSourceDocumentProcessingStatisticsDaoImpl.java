package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.google.common.collect.Lists;
import com.mongodb.*;

import eu.europeana.harvester.db.interfaces.LastSourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.domain.report.SubTaskState;
import eu.europeana.harvester.domain.report.SubTaskType;

import org.joda.time.Interval;

import java.util.*;

public class LastSourceDocumentProcessingStatisticsDaoImpl implements LastSourceDocumentProcessingStatisticsDao {

	/**
	 * The Datastore interface provides type-safe methods for accessing and
	 * storing your java objects in MongoDB. It provides get/find/save/delete
	 * methods for working with your java objects.
	 */
	private final Datastore datastore;
	private final static int THRESHOLD = 1000;
	public LastSourceDocumentProcessingStatisticsDaoImpl(Datastore datastore) {
		this.datastore = datastore;
	}

	@Override
	public Long getCount() {
		return datastore.getCount(LastSourceDocumentProcessingStatistics.class);
	}

	@Override
	public boolean create(LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatisticss,
			WriteConcern writeConcern) {
		if (read(lastSourceDocumentProcessingStatisticss.getId()) == null) {
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
	public LastSourceDocumentProcessingStatistics read(String sourceDocumentReferenceId,
			DocumentReferenceTaskType taskType, URLSourceType urlSourceType) {
		return read(LastSourceDocumentProcessingStatistics.idOf(sourceDocumentReferenceId, urlSourceType));
	}

	@Override
	public boolean update(
			LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatisticss,
			WriteConcern writeConcern) {
		if (read(lastSourceDocumentProcessingStatisticss.getId()) != null) {
			datastore.save(lastSourceDocumentProcessingStatisticss, writeConcern);
			return true;
		}
		return false;
	}

	@Override
	public com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics> createOrModify(
			LastSourceDocumentProcessingStatistics lastSourceDocumentProcessingStatisticss,
			WriteConcern writeConcern) {
		return datastore.save(lastSourceDocumentProcessingStatisticss, writeConcern);
	}

	@Override
	public Iterable<com.google.code.morphia.Key<LastSourceDocumentProcessingStatistics>> createOrModify(
			Collection<LastSourceDocumentProcessingStatistics> lastSourceDocumentProcessingStatisticss,
			WriteConcern writeConcern) {
		if (null == lastSourceDocumentProcessingStatisticss || lastSourceDocumentProcessingStatisticss.isEmpty()) {
			return Collections.emptyList();
		}
		return datastore.save(lastSourceDocumentProcessingStatisticss, writeConcern);
	}

	@Override
	public WriteResult delete(String id) {
		return datastore.delete(LastSourceDocumentProcessingStatistics.class, id);
	}

	@Override
	public List<LastSourceDocumentProcessingStatistics> findByRecordID( String recordID) {
		final Query<LastSourceDocumentProcessingStatistics> query = datastore.find(LastSourceDocumentProcessingStatistics.class, "referenceOwner.recordId", recordID);
		if (query == null) {
			return new ArrayList<>(0);
		}
		return query.asList();
	}

	@Override
	public Map<ProcessingState, Long> countNumberOfDocumentsWithState() {
		final DBCollection collection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);
		
		final BasicDBList matchElements = new BasicDBList();
		matchElements.add(new BasicDBObject("state", ProcessingState.ERROR.name()));
		matchElements.add(new BasicDBObject("state", ProcessingState.SUCCESS.name()));
		matchElements.add(new BasicDBObject("state", ProcessingState.READY.name()));				
		final BasicDBList matchIsActive = new BasicDBList();
		
		matchIsActive.add(new BasicDBObject("active", true));
		matchIsActive.add(new BasicDBObject("$or", matchElements));
		final DBObject match = new BasicDBObject("$and", matchIsActive);
		
		final DBObject group = new BasicDBObject();
		group.put("_id", "$state");
		group.put("count", new BasicDBObject("$sum", 1));

		final Map<ProcessingState, Long> results = new HashMap<>(ProcessingState.values().length, 1);
		AggregationOutput output = aggregationOutput(collection, match, group);
		if (output != null) {			
			for (final DBObject object : output.results()) {
				long count = ((Number) object.get("count")).longValue();
				results.put(ProcessingState.valueOf((String) object.get("_id")), count);
			}
		}
		return results;
	}

	@Override
	public List<LastSourceDocumentProcessingStatistics> deactivateDocuments(List<String> sourceDocumentReferenceIds, 
			WriteConcern writeConcern) {
		if (null == sourceDocumentReferenceIds || sourceDocumentReferenceIds.isEmpty()) {
			return Collections.emptyList();
		}

		List<LastSourceDocumentProcessingStatistics> docs = new ArrayList<>();
		List<List<String>> split = split(sourceDocumentReferenceIds);
		for(List<String> splitted:split) {
			final Query<LastSourceDocumentProcessingStatistics> query = datastore.find(LastSourceDocumentProcessingStatistics.class);
			query.field("sourceDocumentReferenceId").hasAnyOf(splitted);

			final UpdateOperations<LastSourceDocumentProcessingStatistics> update = datastore
					.createUpdateOperations(LastSourceDocumentProcessingStatistics.class);

			update.set("active", false);
			datastore.update(query, update, false, writeConcern);
		//	docs.addAll(query.asList());
		}
		return docs;
	}

	private List<List<String>> split(List<String> sourceDocumentReferenceIds) {
		return Lists.partition(sourceDocumentReferenceIds,THRESHOLD);
	}

	@Override
	public Map<SubTaskState, Long> countSubTaskStatesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType,
			final SubTaskType subtaskType) {
		final DBCollection processingJobCollection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);
		
		final DBObject match = new BasicDBObject();
		match.put("referenceOwner.collectionId", collectionId);
		match.put("active", true);
		match.put("urlSourceType", urlSourceType.name());

		final DBObject group = new BasicDBObject();
		switch (subtaskType) {
		case COLOR_EXTRACTION:
			group.put("_id", "$processingJobSubTaskStats.colorExtractionState");
			break;
		case META_EXTRACTION:
			group.put("_id", "$processingJobSubTaskStats.metaExtractionState");
			break;
		case RETRIEVE:
			group.put("_id", "$processingJobSubTaskStats.retrieveState");
			break;
		case THUMBNAIL_GENERATION:
			group.put("_id", "$processingJobSubTaskStats.thumbnailGenerationState");
			break;
		case THUMBNAIL_STORAGE:
			group.put("_id", "$processingJobSubTaskStats.thumbnailStorageState");
			break;
		}		
		group.put("total", new BasicDBObject("$sum", 1));
		
		final AggregationOutput output = aggregationOutput(processingJobCollection, match, group);
		final Map<SubTaskState, Long> subTasksCountPerState = new HashMap<>();
		if (output != null) {
			for (DBObject result : output.results()) {
				final String state = (String) result.get("_id");
				final Integer count = (Integer) result.get("total");
				if (state != null)
					subTasksCountPerState.put(SubTaskState.valueOf(state), new Long(count));
			}
		}
		return subTasksCountPerState;
	}
	
	@Override
	public Long countSubTaskStatesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType, SubTaskType subTaskType, SubTaskState subTaskState) {
		final DBCollection collection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);		
		final DBObject count = new BasicDBObject();
		count.put("referenceOwner.collectionId", collectionId);
		count.put("active", true);
		count.put("urlSourceType", urlSourceType.name());
		String subTaskTypeField = null;
		switch (subTaskType) {
		case COLOR_EXTRACTION:
			subTaskTypeField = "colorExtractionState";
			break;
		case META_EXTRACTION:
			subTaskTypeField = "metaExtractionState";
			break;
		case RETRIEVE:
			subTaskTypeField = "retrieveState";
			break;
		case THUMBNAIL_GENERATION:
			subTaskTypeField = "thumbnailGenerationState";
			break;
		case THUMBNAIL_STORAGE:
			subTaskTypeField = "thumbnailStorageState";
			break;
		}
		if (subTaskTypeField != null && subTaskState != null) {
			count.put("processingJobSubTaskStats." + subTaskTypeField, subTaskState.name());			
		}
		return collection.count(count);
	}

	@Override
	public Map<ProcessingState, Long> countJobStatesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType,
			final DocumentReferenceTaskType documentReferenceTaskType) {
		final DBCollection processingJobCollection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);

		final DBObject match = new BasicDBObject();
		match.put("referenceOwner.collectionId", collectionId);
		match.put("active", true);
		match.put("urlSourceType", urlSourceType.name());
		match.put("taskType", documentReferenceTaskType.name());

		final DBObject group = new BasicDBObject("_id", "$state");
		group.put("count", new BasicDBObject("$sum", 1));
		
		final AggregationOutput output = aggregationOutput(processingJobCollection, match, group);
		final Map<ProcessingState, Long> jobStateCount = new HashMap<>();
		if (output != null) {
			for (DBObject result : output.results()) {
				final String state = (String) result.get("_id");
				final Integer count = (Integer) result.get("count");
				if (state != null)
					jobStateCount.put(ProcessingState.valueOf(state), new Long(count));
			}
		}
		return jobStateCount;
	}
	
	@Override
	public Long countAllTaskTypesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType) {
		final DBCollection collection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);		
		final DBObject count = new BasicDBObject();
		count.put("referenceOwner.collectionId", collectionId);
		count.put("active", true);
		count.put("urlSourceType", urlSourceType.name());		
		return collection.count(count);
	}

	@Override
	public Long countSuccessfulTaskTypesByUrlSourceType(final String collectionId, final URLSourceType urlSourceType) {
		final DBCollection collection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);		
		final DBObject count = new BasicDBObject();
		count.put("referenceOwner.collectionId", collectionId);
		count.put("active", true);
		count.put("urlSourceType", urlSourceType.name());
		final DBObject in = new BasicDBObject();
		String[] states = new String[]{ProcessingState.SUCCESS.name(),ProcessingState.FAILED.name()};
		in.put("$in", states);
		count.put("state", in);
		return collection.count(count);
	}

	@Override
	public List<LastSourceDocumentProcessingStatistics> findLastSourceDocumentProcessingStatistics(
			final String collectionId, final String executionId,
			final List<ProcessingState> processingStates) {
		final Query<LastSourceDocumentProcessingStatistics> query = datastore.find(LastSourceDocumentProcessingStatistics.class);
		query.field("referenceOwner.collectionId").equal(collectionId);
		if (executionId != null) {
			query.field("referenceOwner.executionId").equal(executionId);
		}
		// The state
		if (processingStates != null && !processingStates.isEmpty()) {
			final List<String> s = new ArrayList<>();
			for (ProcessingState state : processingStates) {
				s.add(state.name());
			}
			query.field("state").hasAnyOf(s);
		}
		query.field("active").equal(true);
		return query.asList();
	}

	@Override
	public Map<JobState, Long> countProcessingJobsByState(final String executionId) {
		final DBCollection processingJobCollection = datastore.getCollection(LastSourceDocumentProcessingStatistics.class);
		
		final DBObject match = new BasicDBObject();
		match.put("referenceOwner.executionId", executionId);
		match.put("active", true);

		final DBObject group = new BasicDBObject();
		group.put("_id", "$state");
		group.put("total", new BasicDBObject("$sum", 1));

		final AggregationOutput output = aggregationOutput(processingJobCollection, match, group);
		final Map<JobState, Long> jobsPerState = new HashMap<>();

		if (output != null) {
			for (DBObject result : output.results()) {
				final String state = (String) result.get("_id");
				final Integer count = (Integer) result.get("total");
				if (state != null)
					jobsPerState.put(JobState.valueOf(state), new Long(count));
			}
		}
		return jobsPerState;
	}

	@Override
	public Interval getDateIntervalForProcessing(String executionId) {

		final Query<LastSourceDocumentProcessingStatistics> query = datastore.find(LastSourceDocumentProcessingStatistics.class);
		query.field("referenceOwner.executionId").equal(executionId);
		query.limit(1);

		query.order("createdAt");
		LastSourceDocumentProcessingStatistics firstCreated = query.get();
		
		final Query<LastSourceDocumentProcessingStatistics> queryLast = datastore.find(LastSourceDocumentProcessingStatistics.class);
		queryLast.field("referenceOwner.executionId").equal(executionId);
		queryLast.limit(1);
		queryLast.order("-updatedAt");
		LastSourceDocumentProcessingStatistics lastUpdated = queryLast.get();
		if(firstCreated.getCreatedAt()!=null && lastUpdated.getUpdatedAt() !=null) {
			return new Interval(firstCreated.getCreatedAt().getTime(), lastUpdated.getUpdatedAt().getTime());
		}
		return new Interval(0,0);
	}
	
	/**
	 * Utility method for grouping results.
	 * TODO: replace the deprecated code with an actual one!
	 * @param dbCollection
	 * @param match
	 * @param group
	 * @return
	 */
	private AggregationOutput aggregationOutput(final DBCollection dbCollection, final DBObject match, final DBObject group) {
		return dbCollection.aggregate(new BasicDBObject("$match", match), new BasicDBObject("$group", group));
	}


}