package eu.europeana.crfmigration.logic;

import com.codahale.metrics.Timer;
import com.mongodb.DBCursor;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.crfmigration.dao.MigratorEuropeanaDao;
import eu.europeana.crfmigration.dao.MigratorHarvesterDao;
import eu.europeana.crfmigration.domain.EuropeanaEDMObject;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;
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
    private final MigratorMetrics metrics;

    private final MigratorEuropeanaDao migratorEuropeanaDao;
    private final MigratorHarvesterDao migratorHarvesterDao;

    private final Date dateFilter;

    private final int batch;

    public MigrationManager(final MigratorEuropeanaDao migratorEuropeanaDao, final MigratorHarvesterDao migratorHarvesterDao, final MigratorMetrics metrics, final Date dateFilter, final int batch) throws IOException {
        this.migratorEuropeanaDao = migratorEuropeanaDao;
        this.migratorHarvesterDao = migratorHarvesterDao;
        this.metrics = metrics;
        this.dateFilter = dateFilter;
        this.batch = batch;
    }

    public void migrate() {
        try {
            metrics.startTotalMigrationTimer();
            starMigration();
        } finally {
            for (final Timer.Context context : metrics.getAllTimeContexts()) {
                context.stop();
            }
        }
    }

    private void starMigration() {
        LOG.info("start migration");
        DBCursor recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter);

        int positionInRecordCollection = 0;
        metrics.logBatchSize(batch);

        while (recordCursor.hasNext()) {
            try {
                final Map<String, String> recordsRetrievedInBatch = migratorEuropeanaDao.retrieveRecordsIdsFromCursor(recordCursor, batch);
                positionInRecordCollection = positionInRecordCollection + recordsRetrievedInBatch.size();
                metrics.logBatchDocumentProcessing(recordsRetrievedInBatch.size());

                try {
                    metrics.startSourceDocumentReferencesTimer();
                    migrateRecordsInSingleBatch(recordsRetrievedInBatch);
                    metrics.stopSourceDocumentReferencesTimer();
                } catch (Exception e) {
                    metrics.incBatchProcessingError();
                    LOG.info(e);
                }

            } catch (Exception e) {
                LOG.error("Error reading record after reacord: #" + positionInRecordCollection + "\n");
                metrics.incErrorReadingRecord();

                recordCursor = migratorEuropeanaDao.buildRecordsRetrievalCursorByFilter(dateFilter);
                recordCursor.skip(positionInRecordCollection);

            }
        }
        LOG.info("finished migration");
    }

    private void migrateRecordsInSingleBatch(final Map<String, String> recordsInBatch) throws MalformedURLException, UnknownHostException {
        // Retrieve records and convert to jobs
        final List<EuropeanaEDMObject> edmObjectsOfRecords = migratorEuropeanaDao.retrieveSourceDocumentReferences(recordsInBatch);
        final List<ProcessingJobTuple> processingJobTuples = convertEDMObjectToJobs(edmObjectsOfRecords);
        final List<ProcessingJob> processingJobs = ProcessingJobTuple.processingJobsFromList(processingJobTuples);
        final List<SourceDocumentReference> sourceDocumentReferences = ProcessingJobTuple.sourceDocumentReferencesFromList(processingJobTuples);

        // Save them
        metrics.logNumberOfProcessingJobsPerBatch(processingJobs.size());
        metrics.logNumberOfSourceDocumentReferencesPerBatch(sourceDocumentReferences.size());

        migratorHarvesterDao.saveSourceDocumentReferences(sourceDocumentReferences);
        migratorHarvesterDao.saveProcessingJobs(processingJobs);

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
                metrics.incInvalidUrl();
            } catch (MalformedURLException e1) {
                LOG.error("Exception caught during record processing: " + e1.getMessage(), e1);
                metrics.incInvalidUrl();
            }
        }
        return results;
    }

}