package eu.europeana.crfmigration.logic;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

public class MigrationMetrics {

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    public static final String COUNTER = "counter";
    public static final String DURATION = "duration";

    public static final String TOTAL = "total";

    public static class Migrator {

        public static String NAME = "MigratorMetrics" + "." + "Migrator";

        public static class Overall {
            public static String NAME = Migrator.NAME + "." + "Overall";

            public static final Counter invalidUrlCounter = METRIC_REGISTRY.counter(name(NAME, "invalidUrl",COUNTER));

            public static final Counter processedRecordsCount = METRIC_REGISTRY.counter(name(NAME, "processedRecords",COUNTER));
            public static final Counter processedSourceDocumentReferencesCount = METRIC_REGISTRY.counter(name(NAME, "processedSourceDocumentReferences",COUNTER));
            public static final Counter processedJobsCount = METRIC_REGISTRY.counter(name(NAME, "processedJobs",COUNTER));
        }

        public static class Batch {
            public static String NAME = Migrator.NAME + "." + "Batch";
            public static final Counter skippedBecauseOfErrorCounter = METRIC_REGISTRY.counter(name(NAME, "skippedBecauseOfError",COUNTER));

            public static final Timer processedRecordsDuration = METRIC_REGISTRY.timer(name(NAME, "processedRecords",DURATION));
            public static final Timer processedRecordsAggregationDuration = METRIC_REGISTRY.timer(name(NAME, "processedRecordsAggregation",DURATION));

            public static final Timer processedSourceDocumentReferencesDuration = METRIC_REGISTRY.timer(name(NAME, "processedSourceDocumentReferences", DURATION));

            public static final Timer processedJobsDuration = METRIC_REGISTRY.timer(name(NAME, "processedJobs", DURATION));

            public static final Timer totalDuration = METRIC_REGISTRY.timer(name(NAME, TOTAL,DURATION));

        }

    }

}