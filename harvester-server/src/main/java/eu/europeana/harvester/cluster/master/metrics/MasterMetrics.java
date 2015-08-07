package eu.europeana.harvester.cluster.master.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.domain.ProcessingJobSubTaskState;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.monitoring.LazyGauge;

import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by paul on 04/06/15.
 */
public class MasterMetrics {
    public static final String COUNTER = "counter";
    public static final String DURATION = "duration";

    public static final String TOTAL = "total";

    public static final String SEND_JOBS_SET_TO_SLAVE = "sendJobsSetToSlave";
    public static final String LOAD_JOBS_FROM_DB = "loadJobsFromDB";
    public static final String LOAD_JOBS_TASKS_FROM_DB = "loadJobsTasksFromDB";
    public static final String LOAD_JOBS_RESOURCES_FROM_DB = "loadJobsResourcesFromDB";
    public static final String LOAD_FASTLANEJOBS_FROM_DB = "loadFastLaneJobsFromDB";
    public static final String LOAD_FASTLANEJOBS_TASKS_FROM_DB = "loadFastLaneJobsTasksFromDB";
    public static final String LOAD_FASTLANEJOBS_RESOURCES_FROM_DB = "loadFastLaneJobsResourcesFromDB";

    public static final String DONE_DOWNLOAD = "doneDownload";
    public static final String JOBS_PERSISTENCE = "jobsPersistence";

    public static final String DONE_PROCESSING = "doneProcessing";

    public static final String DONE_PROCESSING_RETRIEVE = "doneProcessing.retrieve";
    public static final String DONE_PROCESSING_COLOR_EXTRACTION = "doneProcessing.colorExtraction";
    public static final String DONE_PROCESSING_META_EXTRACTION = "doneProcessing.metaExtraction";
    public static final String DONE_PROCESSING_THUMBNAIL_GENERATION = "doneProcessing.thumbnailGeneration";
    public static final String DONE_PROCESSING_THUMBNAIL_STORAGE = "doneProcessing.thumbnailStorage";

    public static final String CONNECTED_NODES_IN_CLUSTER = "connectedNodesInCluster";

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    public static class Master {
        public static final String NAME = "MasterMetrics" + "." + "Master";
        public static final Timer sendJobSetToSlaveDuration = METRIC_REGISTRY.timer(name(Master.NAME, SEND_JOBS_SET_TO_SLAVE, DURATION));
        public static final Counter sendJobSetToSlaveCounter = METRIC_REGISTRY.counter(name(Master.NAME, SEND_JOBS_SET_TO_SLAVE, COUNTER));
        public static final Timer loadJobFromDBDuration = METRIC_REGISTRY.timer(name(Master.NAME, LOAD_JOBS_FROM_DB, COUNTER));

        public static final Timer loadJobTasksFromDBDuration = METRIC_REGISTRY.timer(name(Master.NAME, LOAD_JOBS_TASKS_FROM_DB, COUNTER));
        public static final Timer loadJobResourcesFromDBDuration = METRIC_REGISTRY.timer(name(Master.NAME, LOAD_JOBS_RESOURCES_FROM_DB, COUNTER));

        public static final Timer loadFastLaneJobTasksFromDBDuration = METRIC_REGISTRY.timer(name(Master.NAME, LOAD_FASTLANEJOBS_TASKS_FROM_DB, COUNTER));
        public static final Timer loadFastLaneJobResourcesFromDBDuration = METRIC_REGISTRY.timer(name(Master.NAME, LOAD_FASTLANEJOBS_RESOURCES_FROM_DB, COUNTER));

        public static final LazyGauge jobsPersistenceReadyCount = new LazyGauge(METRIC_REGISTRY, Master.NAME + "." + JOBS_PERSISTENCE + "." + "READY" + "." + COUNTER);
        public static final LazyGauge jobsPersistenceFinishedWithSuccessCount = new LazyGauge(METRIC_REGISTRY, Master.NAME + "." + JOBS_PERSISTENCE + "." + "FINISHED_SUCCESS" + "." + COUNTER);
        public static final LazyGauge jobsPersistenceErrorCount = new LazyGauge(METRIC_REGISTRY, Master.NAME + "." + JOBS_PERSISTENCE + "." + "FINISHED_ERROR" + "." + COUNTER);

        public static final LazyGauge connectedNodesInClusterCount = new LazyGauge(METRIC_REGISTRY, name(Master.NAME, CONNECTED_NODES_IN_CLUSTER, COUNTER));

        public static final Map<RetrievingState, Meter> doneDownloadStateCounters = new HashMap();

        static {
            for (final RetrievingState state : RetrievingState.values()) {
                doneDownloadStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_DOWNLOAD, state.name(), COUNTER)));
            }
        }

        public static final Meter doneDownloadTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_DOWNLOAD, TOTAL, COUNTER));

        public static final Map<ProcessingState, Meter> doneProcessingStateCounters = new HashMap();

        static {
            for (final ProcessingState state : ProcessingState.values()) {
                doneProcessingStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING, TOTAL, COUNTER));

        // Sub tasks counters

        /* RETRIEVE SUB TASK */
        public static final Map<ProcessingJobSubTaskState, Meter> doneProcessingRetrieveStateCounters = new HashMap();

        static {
            for (final ProcessingJobSubTaskState state : ProcessingJobSubTaskState.values()) {
                doneProcessingRetrieveStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_RETRIEVE, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingRetrieveTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_RETRIEVE, TOTAL, COUNTER));

        /* COLOR EXTRACTION SUB TASK */
        public static final Map<ProcessingJobSubTaskState, Meter> doneProcessingColorExtractionStateCounters = new HashMap();

        static {
            for (final ProcessingJobSubTaskState state : ProcessingJobSubTaskState.values()) {
                doneProcessingColorExtractionStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_COLOR_EXTRACTION, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingColorExtractionTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_COLOR_EXTRACTION, TOTAL, COUNTER));

        /* META INFO EXTRACTION SUB TASK */

        public static final Map<ProcessingJobSubTaskState, Meter> doneProcessingMetaExtractionStateCounters = new HashMap();

        static {
            for (final ProcessingJobSubTaskState state : ProcessingJobSubTaskState.values()) {
                doneProcessingMetaExtractionStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_META_EXTRACTION, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingMetaExtractionTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_META_EXTRACTION, TOTAL, COUNTER));

        /* THUMBNAIL GENERATION SUB TASK */

        public static final Map<ProcessingJobSubTaskState, Meter> doneProcessingThumbnailGenerationStateCounters = new HashMap();

        static {
            for (final ProcessingJobSubTaskState state : ProcessingJobSubTaskState.values()) {
                doneProcessingThumbnailGenerationStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_THUMBNAIL_GENERATION, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingThumbnailGenerationTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_THUMBNAIL_GENERATION, TOTAL, COUNTER));


        /* THUMBNAIL STORAGE SUB TASK */

        public static final Map<ProcessingJobSubTaskState, Meter> doneProcessingThumbnailStorageStateCounters = new HashMap();

        static {
            for (final ProcessingJobSubTaskState state : ProcessingJobSubTaskState.values()) {
                doneProcessingThumbnailStorageStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_THUMBNAIL_STORAGE, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingThumbnailStorageTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING_THUMBNAIL_STORAGE, TOTAL, COUNTER));


    }
}
