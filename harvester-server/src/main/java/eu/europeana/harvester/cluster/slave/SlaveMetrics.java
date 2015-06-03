package eu.europeana.harvester.cluster.slave;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.RetrievingState;

import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * The slave metrics.
 */
public class SlaveMetrics {

    public static final String COUNTER = "counter";
    public static final String DURATION = "duration";

    public static final String TOTAL = "total";

    public static final String LINK_CHECKING = "linkChecking";
    public static final String UNCONDITIONAL_DOWNLOAD = "unconditionalDownload";
    public static final String CONDITIONAL_DOWNLOAD = "conditionalDownload";

    public static final String META_INFO_EXTRACTION = "metaInfoExtraction";
    public static final String COLOR_EXTRACTION = "colorExtraction";
    public static final String THUMBNAIL_GENERATION = "thumbnailGeneration";
    public static final String THUMBNAIL_STORAGE = "thumbnailStorage";
    public static final String ORIGINAL_CACHING = "originalCaching";

    public static final String DONE_DOWNLOAD = "doneDownload";
    public static final String DONE_PROCESSING = "doneProcessing";

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    public static class Worker {

        public static String NAME = "SlaveMetrics" + "." + "Worker";

        public static class Master {

            public static String NAME = Worker.NAME + "." + "Master";

            public static final Counter activeWorkerSlavesCounter = METRIC_REGISTRY.counter(name(Master.NAME, "actors", "size"));

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

        }

        public static class Slave {

            public static String NAME = Worker.NAME + "." + "Slave";

            /**
             * How many times has the circuit breaker closed inside the worker.
             */
            public static final Counter forcedSelfDestructCounter = METRIC_REGISTRY.counter(name(Slave.NAME, "forcedTerminationTimeout", COUNTER));


            public static class Retrieve {

                public static String NAME = Slave.NAME + "." + "Retrieve";

                public static final Timer totalDuration = METRIC_REGISTRY.timer(name(Retrieve.NAME, TOTAL, DURATION));

                public static final Timer linkCheckingDuration = METRIC_REGISTRY.timer(name(Retrieve.NAME, LINK_CHECKING, DURATION));
                public static final Counter linkCheckingCounter = METRIC_REGISTRY.counter(name(Retrieve.NAME, LINK_CHECKING, COUNTER));

                public static final Timer unconditionalDownloadDuration = METRIC_REGISTRY.timer(name(Retrieve.NAME, UNCONDITIONAL_DOWNLOAD, DURATION));
                public static final Counter unconditionalDownloadCounter = METRIC_REGISTRY.counter(name(Retrieve.NAME, UNCONDITIONAL_DOWNLOAD, COUNTER));

                public static final Timer conditionalDownloadDuration = METRIC_REGISTRY.timer(name(Retrieve.NAME, CONDITIONAL_DOWNLOAD, DURATION));
                public static final Counter conditionalDownloadCounter = METRIC_REGISTRY.counter(name(Retrieve.NAME, CONDITIONAL_DOWNLOAD, COUNTER));

            }

            public static class Processing {

                public static String NAME = Slave.NAME + "." + "Processing";

                public static final Timer totalDuration = METRIC_REGISTRY.timer(name(Retrieve.NAME, TOTAL, DURATION));

                public static final Timer metaInfoExtractionDuration = METRIC_REGISTRY.timer(name(Processing.NAME, META_INFO_EXTRACTION, DURATION));
                public static final Counter metaInfoExtractionCounter = METRIC_REGISTRY.counter(name(Processing.NAME, META_INFO_EXTRACTION, COUNTER));

                public static final Timer thumbnailGenerationDuration = METRIC_REGISTRY.timer(name(Processing.NAME, THUMBNAIL_GENERATION, DURATION));
                public static final Counter thumbnailGenerationCounter = METRIC_REGISTRY.counter(name(Processing.NAME, THUMBNAIL_GENERATION, COUNTER));

                public static final Timer colorExtractionDuration = METRIC_REGISTRY.timer(name(Processing.NAME, COLOR_EXTRACTION, DURATION));
                public static final Counter colorExtractionCounter = METRIC_REGISTRY.counter(name(Processing.NAME, COLOR_EXTRACTION, COUNTER));

                public static final Timer thumbnailStorageDuration = METRIC_REGISTRY.timer(name(Processing.NAME, THUMBNAIL_STORAGE, DURATION));
                public static final Counter thumbnailStorageCounter = METRIC_REGISTRY.counter(name(Processing.NAME, THUMBNAIL_STORAGE, COUNTER));

                public static final Timer originalCachingDuration = METRIC_REGISTRY.timer(name(Processing.NAME, ORIGINAL_CACHING, DURATION));
                public static final Counter originalCachingCounter = METRIC_REGISTRY.counter(name(Processing.NAME, ORIGINAL_CACHING, COUNTER));

            }
        }
    }
}
