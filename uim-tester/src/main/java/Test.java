import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import com.mongodb.*;
import eu.europeana.harvester.db.interfaces.LastSourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.LastSourceDocumentProcessingStatistics;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.URLSourceType;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by andra on 08.09.2016.
 */
public class Test {

    static List<String> fixedList = new ArrayList<String>();
    static int count = 0;

    static {
        for (DocumentReferenceTaskType taskType : DocumentReferenceTaskType.values()) {
            for (URLSourceType urlSourceType : URLSourceType.values()) {
                fixedList.add("-" + taskType + "-" + urlSourceType);
            }
        }
    }

    private static List<String> getIdsFromSourceDocumentReference(final Datastore dataStore,final int skipAmount,final int limitAmount) {
        final List<String> results = new ArrayList<>();
        for (final SourceDocumentReference o :  dataStore.createQuery(SourceDocumentReference.class).offset(skipAmount).limit(limitAmount).retrievedFields(true, "_id").asList() ) results.add(o.getId());
        return results;
    }

    private static List<String> getCombinations(String sourceRefId){
        List<String> combinationsList = new ArrayList<String>();
        for (String obj : fixedList) {
            combinationsList.add(sourceRefId + obj);
        }

        return combinationsList;
    }

    private static List<LastSourceDocumentProcessingStatistics> getLastStats(final Datastore dataStore, final String sourceDocRefId) {
        Query<LastSourceDocumentProcessingStatistics> query = dataStore.createQuery(LastSourceDocumentProcessingStatistics.class);
        query.field("_id").contains(sourceDocRefId);

        return query.asList();
    }

    private static List<LastSourceDocumentProcessingStatistics> process(final Datastore dataStore,final String sourceDocumentReferenceId) {

        final List<LastSourceDocumentProcessingStatistics> lastStats = getLastStats(dataStore, sourceDocumentReferenceId);

        if (lastStats.size() > 1) {
            count += lastStats.size();

            lastStats.sort(new Comparator<LastSourceDocumentProcessingStatistics>() {
                @Override
                public int compare(LastSourceDocumentProcessingStatistics o1, LastSourceDocumentProcessingStatistics o2) {
                    return -o1.getCreatedAt().compareTo(o2.getCreatedAt());
                }
            });
        }

        return lastStats;

    }

    public static void main(String args[]) throws IOException, InterruptedException, TimeoutException, ExecutionException {

        long start = System.currentTimeMillis();
        MongoClient mongo = new MongoClient("mongo1.crf.europeana.eu", 27017);
        Morphia morphia = new Morphia();
        String dbName = "crf_harvester_second";
        Datastore dataStore = morphia.createDatastore(mongo, dbName);

        DBCollection newStatisticsCollection = dataStore.getDB().getCollection("LastSourceDocumentProcessingStatisticsFiltered");

        int skipAmount = 0;
        int limitAmount = 100 * 100;
        boolean hasMore = true;
        int count = 0;


        while (hasMore) {

            final List<String> sourceDocumentReferenceIds = getIdsFromSourceDocumentReference(dataStore,skipAmount,limitAmount);
            for (final String id : sourceDocumentReferenceIds ) {
                List<LastSourceDocumentProcessingStatistics> processedList = process(dataStore,id);

                if (processedList != null && processedList.size() > 0) {
                    LastSourceDocumentProcessingStatistics neededLastStat = processedList.get(0);
                    count += processedList.size();

                    Map<String, Object> fieldsMap = new HashMap<String, Object>();
                    fieldsMap.put("_id", neededLastStat.getSourceDocumentReferenceId() + "-" + neededLastStat.getUrlSourceType());
                    fieldsMap.put("createdAt", neededLastStat.getCreatedAt());
                    fieldsMap.put("updatedAt", neededLastStat.getUpdatedAt());
                    fieldsMap.put("active", neededLastStat.getActive());
                    fieldsMap.put("taskType", neededLastStat.getTaskType().name());
                    fieldsMap.put("state", neededLastStat.getState().name());

                    DBObject composedRefOwnObj = new BasicDBObject();
                    composedRefOwnObj.put("collectionId", neededLastStat.getReferenceOwner().getCollectionId());
                    if (neededLastStat.getReferenceOwner().getExecutionId() != null) {
                        composedRefOwnObj.put("executionId", neededLastStat.getReferenceOwner().getExecutionId());
                    }
                    composedRefOwnObj.put("providerId", neededLastStat.getReferenceOwner().getProviderId());
                    composedRefOwnObj.put("recordId", neededLastStat.getReferenceOwner().getRecordId());

                    fieldsMap.put("referenceOwner", composedRefOwnObj);
                    fieldsMap.put("urlSourceType", neededLastStat.getUrlSourceType().name());
                    fieldsMap.put("sourceDocumentReferenceId", neededLastStat.getSourceDocumentReferenceId());
                    fieldsMap.put("processingJobId", neededLastStat.getProcessingJobId());
                    fieldsMap.put("httpResponseCode", neededLastStat.getHttpResponseCode());
                    fieldsMap.put("httpResponseContentType", neededLastStat.getHttpResponseContentType());
                    fieldsMap.put("httpResponseContentSizeInBytes", neededLastStat.getHttpResponseContentSizeInBytes());
                    fieldsMap.put("socketConnectToDownloadStartDurationInMilliSecs", neededLastStat.getSocketConnectToDownloadStartDurationInMilliSecs());
                    fieldsMap.put("retrievalDurationInMilliSecs", neededLastStat.getRetrievalDurationInMilliSecs());
                    fieldsMap.put("checkingDurationInMilliSecs", neededLastStat.getCheckingDurationInMilliSecs());
                    fieldsMap.put("sourceIp", neededLastStat.getSourceIp());
                    fieldsMap.put("httpResponseHeaders", neededLastStat.getHttpResponseHeaders());
                    fieldsMap.put("log", neededLastStat.getLog());

                    DBObject composedProcJobSubTaskObj = new BasicDBObject();
                    if (neededLastStat.getProcessingJobSubTaskStats().getColorExtractionLog() != null) {
                        composedProcJobSubTaskObj.put("colorExtractionLog", neededLastStat.getProcessingJobSubTaskStats().getColorExtractionLog());
                    }

                    if (neededLastStat.getProcessingJobSubTaskStats().getMetaExtractionLog() != null) {
                        composedProcJobSubTaskObj.put("metaExtractionLog", neededLastStat.getProcessingJobSubTaskStats().getMetaExtractionLog());
                    }

                    if (neededLastStat.getProcessingJobSubTaskStats().getRetrieveLog() != null) {
                        composedProcJobSubTaskObj.put("retrieveLog", neededLastStat.getProcessingJobSubTaskStats().getRetrieveLog());
                    }

                    if (neededLastStat.getProcessingJobSubTaskStats().getThumbnailGenerationLog() != null) {
                        composedProcJobSubTaskObj.put("thumbnailGenerationLog", neededLastStat.getProcessingJobSubTaskStats().getThumbnailGenerationLog());
                    }

                    if (neededLastStat.getProcessingJobSubTaskStats().getThumbnailStorageLog() != null) {
                        composedProcJobSubTaskObj.put("thumbnailStorageLog", neededLastStat.getProcessingJobSubTaskStats().getThumbnailStorageLog());
                    }

                    composedProcJobSubTaskObj.put("colorExtractionState", neededLastStat.getProcessingJobSubTaskStats().getColorExtractionState().name());
                    composedProcJobSubTaskObj.put("overallState", neededLastStat.getProcessingJobSubTaskStats().getOverallState().name());
                    composedProcJobSubTaskObj.put("metaExtractionState", neededLastStat.getProcessingJobSubTaskStats().getMetaExtractionState().name());
                    composedProcJobSubTaskObj.put("thumbnailGenerationState", neededLastStat.getProcessingJobSubTaskStats().getThumbnailGenerationState().name());
                    composedProcJobSubTaskObj.put("thumbnailStorageState", neededLastStat.getProcessingJobSubTaskStats().getThumbnailStorageState().name());
                    composedProcJobSubTaskObj.put("retrieveState", neededLastStat.getProcessingJobSubTaskStats().getRetrieveState().name());

                    fieldsMap.put("processingJobSubTaskStats", composedProcJobSubTaskObj);

                    DBObject newCollectionRecord = new BasicDBObject(fieldsMap);
                    newStatisticsCollection.insert(newCollectionRecord);


                }
            }

            skipAmount+=sourceDocumentReferenceIds.size();
            System.out.println("+++++++++++++++++++++++++++++++++ skipAmount: " + skipAmount);
            hasMore = (sourceDocumentReferenceIds.size() == limitAmount);
        }

        System.out.println("Total records read : " + skipAmount);
        System.out.println("New collection total count : " + newStatisticsCollection.count());
        long stop = System.currentTimeMillis();
        long total = stop-start;
        System.out.println("Time: " + total + " milisec");
        System.out.println("count: " + count);
    }
}
