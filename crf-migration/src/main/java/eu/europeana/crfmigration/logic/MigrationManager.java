package eu.europeana.crfmigration.logic;

import com.codahale.metrics.Timer;
import com.mongodb.DBCursor;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Creates the processing jobs and the source documents needed by them.
 */
public class MigrationManager {

    private static final Logger LOG = LogManager.getLogger(MigrationManager.class.getName());

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
        LOG.info("start migration");
        DBCursor recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter);

        int positionInRecordCollection = 0;

        while (recordCursor.hasNext()) {
            final Timer.Context totalTimerContext = MigrationMetrics.Migrator.Batch.totalDuration.time();
            try {
                Map<String, String> recordsRetrievedInBatch = null;
                final Timer.Context processedRecordsDurationTimerContext = MigrationMetrics.Migrator.Batch.processedRecordsDuration.time();
                try {
                    recordsRetrievedInBatch = migratorEuropeanaDao.retrieveRecordsIdsFromCursor(recordCursor, batch);
                    positionInRecordCollection = positionInRecordCollection + recordsRetrievedInBatch.size();
                } finally {
                    processedRecordsDurationTimerContext.stop();
                }
                migrateRecordsInSingleBatch(recordsRetrievedInBatch);
                MigrationMetrics.Migrator.Overall.processedRecordsCount.inc(recordsRetrievedInBatch.size());
            } catch (Exception e) {
                LOG.error("Error reading record after reacord: #" + positionInRecordCollection + "\n");
                MigrationMetrics.Migrator.Batch.skippedBecauseOfErrorCounter.inc();
                recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter);
                recordCursor.skip(positionInRecordCollection);

            } finally {
                totalTimerContext.stop();
            }
        }
        LOG.info("finished migration");
    }

    private void migrateRecordsInSingleBatch(final Map<String, String> recordsInBatch) throws MalformedURLException, UnknownHostException {
        // Retrieve records and convert to jobs
        List<EuropeanaEDMObject> edmObjectsOfRecords = null;
        final Timer.Context processedRecordsAggregationTimerContext = MigrationMetrics.Migrator.Batch.processedRecordsAggregationDuration.time();
        try {
            migratorEuropeanaDao.retrieveAggregationEDMInformation(recordsInBatch);
        } finally {
            processedRecordsAggregationTimerContext.stop();
        }

        final List<ProcessingJobTuple> processingJobTuples = convertEDMObjectToJobs(edmObjectsOfRecords);
        final List<ProcessingJob> processingJobs = ProcessingJobTuple.processingJobsFromList(processingJobTuples);
        final List<SourceDocumentReference> sourceDocumentReferences = ProcessingJobTuple.sourceDocumentReferencesFromList(processingJobTuples);

        // Save the jobs
        final Timer.Context processedJobsTimerContext = MigrationMetrics.Migrator.Batch.processedJobsDuration.time();
        try {
            migratorHarvesterDao.saveProcessingJobs(processingJobs);
            MigrationMetrics.Migrator.Overall.processedJobsCount.inc(sourceDocumentReferences.size());
        } finally {
            processedJobsTimerContext.stop();
        }
    }

    private List<ProcessingJobTuple> convertEDMObjectToJobs(final List<EuropeanaEDMObject> edmObjects) {
        final List<ProcessingJobTuple> results = new ArrayList();
        for (final EuropeanaEDMObject edmObject : edmObjects) {
            try {
                results.addAll(
                        JobCreator.createJobs(edmObject.getReferenceOwner().getCollectionId(),
                                edmObject.getReferenceOwner().getProviderId(),
                                edmObject.getReferenceOwner().getRecordId(),
                                edmObject.getReferenceOwner().getExecutionId(),
                                edmObject.getEdmObject(), edmObject.getEdmHasViews(), edmObject.getEdmIsShownBy(), edmObject.getEdmIsShownAt()));
            } catch (UnknownHostException e1) {
                LOG.error("Exception caught during record processing: " + e1.getMessage(), e1);
                MigrationMetrics.Migrator.Overall.invalidUrlCounter.inc();
            } catch (MalformedURLException e1) {
                LOG.error("Exception caught during record processing: " + e1.getMessage(), e1);
                MigrationMetrics.Migrator.Overall.invalidUrlCounter.inc();
            }
        }
        return results;
    }

}