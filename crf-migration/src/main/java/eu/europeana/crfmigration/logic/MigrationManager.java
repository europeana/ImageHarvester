package eu.europeana.crfmigration.logic;

import com.codahale.metrics.Timer;
import com.mongodb.DBCursor;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.crfmigration.domain.EuropeanaRecord;
import eu.europeana.crfmigration.logging.LoggingComponent;
import eu.europeana.harvester.domain.JobPriority;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    public MigrationManager(final MigratorEuropeanaDao migratorEuropeanaDao, final MigratorHarvesterDao migratorHarvesterDao, final Date dateFilter, final int batch) throws IOException {
        this.migratorEuropeanaDao = migratorEuropeanaDao;
        this.migratorHarvesterDao = migratorHarvesterDao;
        this.dateFilter = dateFilter;
        this.batch = batch;
    }

    public void migrate() {
        starMigration();
    }

    private void starMigration() {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                "Started migration process with minimum date filter {}", dateFilter);
        DBCursor recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter,null);

        int positionInRecordCollection = 0;

        while (recordCursor.hasNext()) {
            final String migratingBatchId = "migrating-batch-"+ DateTime.now().getMillis()+"-"+Math.random();
            DateTime minimalUpdatedTimestampInRecords = null;
            final Timer.Context totalTimerContext = MigrationMetrics.Migrator.Batch.totalDuration.time();
            try {
                Map<String, EuropeanaRecord> recordsRetrievedInBatch = null;
                final Timer.Context processedRecordsDurationTimerContext = MigrationMetrics.Migrator.Batch.processedRecordsDuration.time();
                try {
                    recordsRetrievedInBatch = migratorEuropeanaDao.retrieveRecordsIdsFromCursor(recordCursor, batch,migratingBatchId);
                    minimalUpdatedTimestampInRecords = EuropeanaRecord.minimalTimestampUpdated(recordsRetrievedInBatch.values());
                    positionInRecordCollection = positionInRecordCollection + recordsRetrievedInBatch.size();
                } finally {
                    processedRecordsDurationTimerContext.stop();
                }
                migrateRecordsInSingleBatch(recordsRetrievedInBatch,migratingBatchId);
                MigrationMetrics.Migrator.Overall.processedRecordsCount.inc(recordsRetrievedInBatch.size());

                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "Finished migration batch with success to cursor position {} and minimum date in batch was {}.",positionInRecordCollection, minimalUpdatedTimestampInRecords);

                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "Finished migration batch with {} records",recordsRetrievedInBatch.size());

            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,migratingBatchId,null,null),
                        "Error reading record after record: #" + positionInRecordCollection,e);

                MigrationMetrics.Migrator.Batch.skippedBecauseOfErrorCounter.inc();
                recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(minimalUpdatedTimestampInRecords.toDate(),migratingBatchId);
                recordCursor.skip(positionInRecordCollection);

                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "Finished migration batch with error and skipping to cursor position {} . Cursor is being rebuilt.", positionInRecordCollection);

            } finally {
                totalTimerContext.stop();
            }
        }
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

    private List<ProcessingJobTuple> convertEDMObjectToJobs(final List<EuropeanaEDMObject> edmObjects,final String migratingBatchId) throws ExecutionException {
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
            }
        }
        return results;
    }

}