package eu.europeana.harvester.domain;

public class LogMarker {

    public static final String EUROPEANA_PROVIDER_ID = "europeana_provider_id";
    public static final String EUROPEANA_COLLECTION_ID = "europeana_collection_id";
    public static final String EUROPEANA_RECORD_ID = "europeana_record_id";
    public static final String EUROPEANA_PROCESSING_JOB_ID = "europeana_processing_job_id";
    public static final String EUROPEANA_MIGRATING_BATCH_ID = "europeana_migrating_batch_id";
    public static final String EUROPEANA_PUBLISHING_BATCH_ID = "europeana_publishing_batch_id";

    public static final String EUROPEANA_PROCESSING_JOB_EXECUTION_ID = "europeana_processing_job_execution_id";
    public static final String EUROPEANA_SOURCE_DOCUMENT_REFERENCE_ID = "europeana_source_document_reference_id";
    public static final String EUROPEANA_URL = "europeana_url";
    public static final String EUROPEANA_PROCESSING_JOB_TASK_TYPE = "europeana_processing_job_task_type";

    /**
     * The component that is the source of the log message. Examples : slave master, slave worker, slave downloader, slave processor etc.
     */
    public static final String EUROPEANA_COMPONENT = "europeana_component";
    public static final String EUROPEANA_COMPONENT_PROCESS = "europeana_component_process";

}
