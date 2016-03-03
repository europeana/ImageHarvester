package eu.europeana.harvester.client;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import eu.europeana.harvester.client.pagedElements.PagedElements;
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
        return new ProcessingJob(
                o.getString("id"),
                o.getInt("priority"),
                o.getDate("expectedStartDate"),
                getReferenceOwner((BasicDBObject)o.get("referenceOwner")),
                getListOfProcessingJobTaskDocumentReference((BasicDBList)o.get("")),
                JobState.valueOf(o.getString("state")),
                URLSourceType.valueOf(o.getString("urlSourceType")),
                o.getString("ipAddress"),
                o.getBoolean("active"),
                getProcessingJobLimits((BasicDBObject)o.get("limits"))
        );
    }


    private ProcessingJobLimits getProcessingJobLimits(BasicDBObject o) {
        return new ProcessingJobLimits(
                o.getLong("retrievalTerminationThresholdTimeLimitInMillis"),
                o.getLong("retrievalTerminationThresholdReadPerSecondInBytes"),
                o.getLong("retrievalConnectionTimeoutInMillis"),
                o.getInt("retrievalMaxNrOfRedirects"),
                o.getLong("processingTerminationThresholdTimeLimitInMillis")
        );
    }

    private List<ProcessingJobTaskDocumentReference> getListOfProcessingJobTaskDocumentReference(BasicDBList l) {
        final List<ProcessingJobTaskDocumentReference> list = new ArrayList<>();
        for (Object obj: l) {
            final BasicDBObject dbObj = (BasicDBObject)obj;

            list.add(new ProcessingJobTaskDocumentReference(
                    DocumentReferenceTaskType.valueOf(dbObj.getString("taskType")),
                    dbObj.getString("sourceDocumentReferenceId"),
                    getListOfProcessingJobSubTask((BasicDBList)dbObj.get("processingTasks"))
            ));
        }

        return list;
    }

    private List<ProcessingJobSubTask> getListOfProcessingJobSubTask(BasicDBList processingTasks) {
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
        return new GenericSubTaskConfiguration(getThumbnailConfig((BasicDBObject)config.get("thumbnailConfig")));
    }

    private ThumbnailConfig getThumbnailConfig(BasicDBObject thumbnailConfig) {
        return new ThumbnailConfig(
                thumbnailConfig.getInt("width"),
                thumbnailConfig.getInt("height")
        );
    }

    private ReferenceOwner getReferenceOwner(BasicDBObject o) {
        return new ReferenceOwner(
                o.getString("providerId"),
                o.getString("collectionId"),
                o.getString("recordId"),
                o.getString("executionId")
        );
    }
}
