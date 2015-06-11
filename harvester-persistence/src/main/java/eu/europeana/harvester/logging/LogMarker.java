package eu.europeana.harvester.logging;

/**
 * Log field names used across all CRF components.
 */
public class LogMarker {

    /**
     * The provider id of the record.
     */
    public static final String EUROPEANA_PROVIDER_ID = "europeana_provider_id";

    /**
     * The collection id of the record.
     */
    public static final String EUROPEANA_COLLECTION_ID = "europeana_collection_id";

    /**
     * The record id of the record.
     */
    public static final String EUROPEANA_RECORD_ID = "europeana_record_id";

    /**
     * The processing job id that was executed. This will allow to group log statements together
     * that belong to the same processing job.
     */
    public static final String EUROPEANA_PROCESSING_JOB_ID = "europeana_processing_job_id";

    /**
     * The migrating batch id. This will allow to group log statements together that belong to
     * the same batch id.
     */
    public static final String EUROPEANA_MIGRATING_BATCH_ID = "europeana_migrating_batch_id";

    /**
     * The publishing batch id. This will allow to group log statements together that belong to
     * the same batch id.
     */
    public static final String EUROPEANA_PUBLISHING_BATCH_ID = "europeana_publishing_batch_id";

    /**
     * The job execution id of the processing job. This id is provided by the UIM (or other component that
     * creates harvesting jobs and allows to track "external batches".
     */
    public static final String EUROPEANA_PROCESSING_JOB_EXECUTION_ID = "europeana_processing_job_execution_id";

    /**
     * The URL of the resource that was processed.
     */
    public static final String EUROPEANA_URL = "europeana_url";

    /**
     * The component that is the source of the log message. Examples : slave master, slave worker, slave downloader, slave processor etc.
     */
    public static final String EUROPEANA_COMPONENT = "europeana_component";

}
