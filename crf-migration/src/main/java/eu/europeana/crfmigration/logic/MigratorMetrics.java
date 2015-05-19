package eu.europeana.crfmigration.logic;

import com.codahale.metrics.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by salexandru on 19.05.2015.
 */
public class MigratorMetrics {
    public static final MetricRegistry metricRegistry = new MetricRegistry();


    private final Map<Timer, Timer.Context> timerContexts;
    private final Timer sourceDocumentReferencesTimer;
    private final Timer getAggregationTimer;
    private final Timer saveSourceDocumentReferencesTimer;
    private final Timer saveProcessingJobsTimer;
    private final Timer batchProcessingTimer;
    private final Timer totalMigrationTimer;


    private final Counter totalNumberOfSourceDocumentReferences;
    private final Counter totalNumberOfProcessingJobs;
    private final Counter totalNumberOfDocumentProcessed;
    private final Counter emptyAggregationCounter;
    private final Counter invalidUrlCounter;
    private final Counter processingErrorCounter;
    private final Counter batchProcessingErrorCounter;
    private final Counter errorReadingRecordCounter;

    private final Meter numberOfDocumentsProcessed;
    private final Meter numberOfProcessingJobs;
    private final Meter numberOfSourceDocumentReferences;

    public MigratorMetrics() {

        timerContexts = new HashMap<>();

        totalMigrationTimer = metricRegistry.timer(name(MigratorMetrics.class, "Total Migration Time"));
        sourceDocumentReferencesTimer = metricRegistry.timer(name(MigratorMetrics.class, "Creating ProcessJob Time"));
        getAggregationTimer = metricRegistry.timer(name(MigratorMetrics.class, "Retrieving Aggregation Time"));
        saveSourceDocumentReferencesTimer = metricRegistry.timer(name(MigratorMetrics.class, "Saving SourceDocumentReferences Time"));
        saveProcessingJobsTimer = metricRegistry.timer(name(MigratorMetrics.class, "Saving ProcessJob Time"));
        batchProcessingTimer = metricRegistry.timer(name(MigratorMetrics.class, "Batch Record Retrieving Time"));

        totalNumberOfSourceDocumentReferences = metricRegistry.counter(name(MigratorMetrics.class,
                                                                            "Total Number of SourceDocumentReferences Created"));
        totalNumberOfProcessingJobs = metricRegistry.counter(name(MigratorMetrics.class, "Total Number of ProcessJobs Created"));
        totalNumberOfDocumentProcessed = metricRegistry.counter(name(MigratorMetrics.class, "Total Number of Records Processed"));
        emptyAggregationCounter = metricRegistry.counter(name(MigratorMetrics.class, "Empty Aggregations Count"));
        invalidUrlCounter = metricRegistry.counter(name(MigratorMetrics.class, "Invalid Urls Count"));
        processingErrorCounter = metricRegistry.counter(name(MigratorMetrics.class, "Processing Errors Count"));
        batchProcessingErrorCounter = metricRegistry.counter(name(MigratorMetrics.class, "Batch Processing Errors Count"));
        errorReadingRecordCounter = metricRegistry.counter(name(MigratorMetrics.class, "Reading Record Errors Count"));


        numberOfDocumentsProcessed = metricRegistry.meter(name(MigratorMetrics.class, "Number of Records Processed"));
        numberOfProcessingJobs = metricRegistry.meter(name(MigratorMetrics.class, "Number of ProcessJobs Created"));
        numberOfSourceDocumentReferences = metricRegistry.meter(name(MigratorMetrics.class, "Number of SourceDocumentReferences Created"));
    }

    public void startTotalMigrationTimer () {
        startTimer (totalMigrationTimer);
    }

    public void stopTotalMigrationTimer() {
        stopTimer (totalMigrationTimer);
    }

    public Collection<Timer.Context> getAllTimeContexts () {
        return timerContexts.values();
    }

    public void startSourceDocumentReferencesTimer () {
        startTimer(sourceDocumentReferencesTimer);
    }

    public void stopSourceDocumentReferencesTimer () {
        stopTimer(sourceDocumentReferencesTimer);
    }


    public void startGetAggregationTimer () {
        startTimer (getAggregationTimer);
    }

    public void stopGetAggregationTimer () {
        stopTimer (getAggregationTimer);
    }

    public void startSaveSourceDocumentReferencesTimer () {
        startTimer (saveSourceDocumentReferencesTimer);
    }

    public void stopSaveSourceDocumentReferencesTimer () {
        stopTimer (saveSourceDocumentReferencesTimer);
    }

    public void startSaveProcessingJobsTimer () {
        startTimer (saveProcessingJobsTimer);
    }

    public void stopSaveProcessingJobsTimer () {
        stopTimer (saveProcessingJobsTimer);
    }


    public void logBatchSize (final int batch) {
        metricRegistry.register(name(MigratorMetrics.class, "BatchProcessingSize"), new Gauge<Integer>() {
            @Override
            public Integer getValue () {
                return batch;
            }
        });
    }

    public void logBatchDocumentProcessing (int i) {
        totalNumberOfDocumentProcessed.inc(i);
        numberOfDocumentsProcessed.mark(i);
        startTimer (batchProcessingTimer);
    }

    public void logNumberOfSourceDocumentReferencesPerBatch (int size) {
        totalNumberOfSourceDocumentReferences.inc(size);
        numberOfSourceDocumentReferences.mark(size);
    }

    public void logNumberOfProcessingJobsPerBatch (int size) {
        totalNumberOfProcessingJobs.inc(size);
        numberOfProcessingJobs.mark(size);
    }


    public void incEmptyAggregations () {
        emptyAggregationCounter.inc();
    }

    public void incInvalidUrl () {
        invalidUrlCounter.inc();
    }

    public void incProcessingError () {
        processingErrorCounter.inc();
    }

    public void incBatchProcessingError () {
        batchProcessingErrorCounter.inc();
    }

    public void incErrorReadingRecord () {
        errorReadingRecordCounter.inc();
    }

    private void startTimer(Timer timer) {
        stopTimer(timer);
        timerContexts.put(timer, timer.time());
    }

    private void stopTimer(Timer timer) {
        if (timerContexts.containsKey(timer)) {
            timerContexts.remove(timer).stop();
        }
    }
}
