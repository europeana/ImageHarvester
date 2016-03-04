package eu.europeana.harvester.util.pagedElements;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import eu.europeana.harvester.domain.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Alexandru Stefanica, alexandru.stefanica@busymachines.com
 * @since 03 Mar 2016
 */


public class PagedProcessingJobElements extends PagedElements<ProcessingJob> {
    public PagedProcessingJobElements(DBCursor dbCursor, int pageSize) {
        super(dbCursor, pageSize);
    }

    @Override
    protected ProcessingJob extractFromDBObject(BasicDBObject o) {
        if (null == o) return null;
        return new ProcessingJob(
                o.getString("_id"),
                o.getInt("priority"),
                o.getDate("expectedStartDate"),
                getReferenceOwner((BasicDBObject)o.get("referenceOwner")),
                getListOfProcessingJobTaskDocumentReference((BasicDBList)o.get("tasks")),
                null == o.getString("state") ? null : JobState.valueOf(o.getString("state")),
                null == o.getString("urlSourceType") ? null : URLSourceType.valueOf(o.getString("urlSourceType")),
                o.getString("ipAddress"),
                o.getBoolean("active"),
                getProcessingJobLimits((BasicDBObject)o.get("limits"))
        );
    }


    private ProcessingJobLimits getProcessingJobLimits(BasicDBObject o) {
        if (null == o) return null;
        return new ProcessingJobLimits(
                o.getLong("retrievalTerminationThresholdTimeLimitInMillis"),
                o.getLong("retrievalTerminationThresholdReadPerSecondInBytes"),
                o.getLong("retrievalConnectionTimeoutInMillis"),
                o.getInt("retrievalMaxNrOfRedirects"),
                o.getLong("processingTerminationThresholdTimeLimitInMillis")
        );
    }

    private List<ProcessingJobTaskDocumentReference> getListOfProcessingJobTaskDocumentReference(BasicDBList l) {
        if (null == l || l.isEmpty()) {
            return new ArrayList<>();
        }
        final List<ProcessingJobTaskDocumentReference> list = new ArrayList<>();
        for (Object obj: l) {
            final BasicDBObject dbObj = (BasicDBObject)obj;

            list.add(new ProcessingJobTaskDocumentReference(
                    null == dbObj.get("taskType") ? null : DocumentReferenceTaskType.valueOf(dbObj.getString("taskType")),
                    dbObj.getString("sourceDocumentReferenceId"),
                    getListOfProcessingJobSubTask((BasicDBList)dbObj.get("processingTasks"))
            ));
        }

        return list;
    }

    private List<ProcessingJobSubTask> getListOfProcessingJobSubTask(BasicDBList processingTasks) {
        if (null == processingTasks || processingTasks.isEmpty()) {
            return new ArrayList<>();
        }
        final List<ProcessingJobSubTask> subTaskList = new ArrayList<>();

        for (Object obj: processingTasks) {
            final BasicDBObject dbObject = (BasicDBObject)obj;

            subTaskList.add(new ProcessingJobSubTask(
                    ProcessingJobSubTaskType.valueOf(dbObject.getString("taskType")),
                    getGenericSubTaskConfiguration((BasicDBObject)dbObject.get("config"))
            ));
        }

        return subTaskList;
    }

    private GenericSubTaskConfiguration getGenericSubTaskConfiguration(BasicDBObject config) {
        if (null == config) return null;
        return new GenericSubTaskConfiguration(getThumbnailConfig((BasicDBObject)config.get("thumbnailConfig")));
    }

    private ThumbnailConfig getThumbnailConfig(BasicDBObject thumbnailConfig) {
        if (null == thumbnailConfig) return null;
        return new ThumbnailConfig(
                thumbnailConfig.getInt("width"),
                thumbnailConfig.getInt("height")
        );
    }

    private ReferenceOwner getReferenceOwner(BasicDBObject o) {
        if (null == o) return null;
        return new ReferenceOwner(
                o.getString("providerId"),
                o.getString("collectionId"),
                o.getString("recordId"),
                o.getString("executionId")
        );
    }
}
