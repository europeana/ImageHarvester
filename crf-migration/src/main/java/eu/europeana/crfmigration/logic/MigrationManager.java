package eu.europeana.crfmigration.logic;

import com.codahale.metrics.Timer;
import com.mongodb.DBCursor;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.crfmigration.domain.EuropeanaRecord;
import eu.europeana.crfmigration.logging.LoggingComponent;
import eu.europeana.harvester.domain.JobPriority;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Creates the processing jobs and the source documents needed by them.
 */
public class MigrationManager {

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final MigratorEuropeanaDao migratorEuropeanaDao;
    private final MigratorHarvesterDao migratorHarvesterDao;

    private final Date dateFilter;

    private final int batch;

    private Boolean hasMoreRecords = true;

    public MigrationManager(final MigratorEuropeanaDao migratorEuropeanaDao, final MigratorHarvesterDao migratorHarvesterDao, final Date dateFilter, final int batchSize) throws IOException {
        this.migratorEuropeanaDao = migratorEuropeanaDao;
        this.migratorHarvesterDao = migratorHarvesterDao;
        this.dateFilter = dateFilter;
        this.batch = batchSize;
    }

    public void migrate() {
        starMigration();
    }

    private void starMigration() {
        Date maximalUpdatedTimestampInRecords = dateFilter;

        /** Repeat forever */
        while (hasMoreRecords) {
            final String migratingBatchId = "migrating-batch-"+ DateTime.now().getMillis()+"-"+Math.random();
            final Timer.Context totalTimerContext = MigrationMetrics.Migrator.Batch.totalDuration.time();
            final DBCursor recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(maximalUpdatedTimestampInRecords, batch, null);
            Map<String, EuropeanaRecord> recordsRetrievedInBatch = null;
            int numberOfRecordsRetrievedInBatch = 0;
            System.out.println(
                    "[Console] Starting migration batch and minimum date in batch is "+ maximalUpdatedTimestampInRecords);

            try {
                final Timer.Context processedRecordsDurationTimerContext = MigrationMetrics.Migrator.Batch.processedRecordsDuration.time();
                try {
                    recordsRetrievedInBatch = migratorEuropeanaDao.retrieveRecordsIdsFromCursor(recordCursor, migratingBatchId);
                    if (recordsRetrievedInBatch != null) numberOfRecordsRetrievedInBatch = recordsRetrievedInBatch.size();
                    maximalUpdatedTimestampInRecords = EuropeanaRecord.maximalTimestampUpdated(recordsRetrievedInBatch.values());
                    if (recordsRetrievedInBatch.isEmpty()) hasMoreRecords = false;

                    LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                            "Finished retrieving records batch of {} records.",numberOfRecordsRetrievedInBatch);

                } finally {
                    processedRecordsDurationTimerContext.stop();
                }
                migrateRecordsInSingleBatch(recordsRetrievedInBatch,migratingBatchId);
                MigrationMetrics.Migrator.Overall.processedRecordsCount.inc(recordsRetrievedInBatch.size());

                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "Finished migration batch of {} records with success and minimum date in batch was {}.",numberOfRecordsRetrievedInBatch, maximalUpdatedTimestampInRecords);


            } catch (Exception e) {

                MigrationMetrics.Migrator.Batch.skippedBecauseOfErrorCounter.inc();

                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "Finished migration batch with error and min date was", maximalUpdatedTimestampInRecords);

            } finally {
                System.out.println(
                        "[Console] Finished migration batch of "+numberOfRecordsRetrievedInBatch+" records with success and minimum date in batch was "+ maximalUpdatedTimestampInRecords);
                recordCursor.close();
                totalTimerContext.stop();
            }
        }
        System.out.println(
                "[Console] Finished migration process as there are no more records to migrate");

    }

    private void migrateRecordsInSingleBatch(final Map<String, EuropeanaRecord> recordsInBatch,final String migratingBatchId) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        // Retrieve records and convert to jobs
        List<EuropeanaEDMObject> edmObjectsOfRecords = Collections.emptyList();
        final Timer.Context processedRecordsAggregationTimerContext = MigrationMetrics.Migrator.Batch.processedRecordsAggregationDuration.time();
        try {
            edmObjectsOfRecords = migratorEuropeanaDao.retrieveAggregationEDMInformation(recordsInBatch,migratingBatchId);
            MigrationMetrics.Migrator.Overall.processedRecordsAggregationCount.inc(edmObjectsOfRecords.size());
        } finally {
            processedRecordsAggregationTimerContext.stop();
        }

        final Timer.Context processedEDMToJobTuplesConversion = MigrationMetrics.Migrator.Batch.processedEDMToJobTuplesConversionDuration.time();

        final List<ProcessingJobTuple> processingJobTuples = convertEDMObjectToJobs(edmObjectsOfRecords,migratingBatchId);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_CONVERT_RECORD_TO_JOB),
                "Finished generating {} tuples from batch record.",processingJobTuples.size());

        processedEDMToJobTuplesConversion.stop();

        final Timer.Context processedJobTuples = MigrationMetrics.Migrator.Batch.processedJobTuplesDuration.time();

        MigrationMetrics.Migrator.Overall.processedJobsCount.inc(processingJobTuples.size());

        //save jobs
        try {
            migratorHarvesterDao.saveProcessingJobTuples(processingJobTuples, migratingBatchId);
        }
        finally {
           processedJobTuples.stop();
        }
    }

    private List<ProcessingJobTuple> convertEDMObjectToJobs(final List<EuropeanaEDMObject> edmObjects,final String migratingBatchId)  {
        if (null == edmObjects || edmObjects.isEmpty()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_CONVERT_RECORD_TO_JOB,migratingBatchId,null,null),
                    "No jobs to convert.");
            return new ArrayList<>();
        }
        final List<ProcessingJobTuple> results = new ArrayList();
        for (final EuropeanaEDMObject edmObject : edmObjects) {
            try {
                results.addAll(
                        JobCreator.createJobs(edmObject.getReferenceOwner().getCollectionId(),
                                edmObject.getReferenceOwner().getProviderId(),
                                edmObject.getReferenceOwner().getRecordId(),
                                edmObject.getReferenceOwner().getExecutionId(),
                                edmObject.getEdmObject(), edmObject.getEdmHasViews(), edmObject.getEdmIsShownBy(), edmObject.getEdmIsShownAt(), JobPriority.NORMAL.getPriority()));
            } catch (UnknownHostException e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_CONVERT_RECORD_TO_JOB,migratingBatchId,null,null),
                        "Exception while converting record.",e);
                MigrationMetrics.Migrator.Overall.invalidUrlCounter.inc();
            } catch (MalformedURLException e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_CONVERT_RECORD_TO_JOB,migratingBatchId,null,null),
                        "Exception while converting record.",e);
                MigrationMetrics.Migrator.Overall.invalidUrlCounter.inc();
            } catch (ExecutionException e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_CONVERT_RECORD_TO_JOB, migratingBatchId, null, null),
                        "Exception while converting record.", e);
            }
        }
        return results;
    }

}