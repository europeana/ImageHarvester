package eu.europeana.harvester.domain;

public class ProcessingJobSubTaskStats {

    private final ProcessingJobSubTaskState retrieveState;
    private final ProcessingJobSubTaskState colorExtractionState;
    private final ProcessingJobSubTaskState metaExtractionState;
    private final ProcessingJobSubTaskState thumbnailGenerationState;
    private final ProcessingJobSubTaskState thumbnailStorageState;

    public ProcessingJobSubTaskStats(ProcessingJobSubTaskState retrieveState, ProcessingJobSubTaskState colorExtractionState, ProcessingJobSubTaskState metaExtractionState, ProcessingJobSubTaskState thumbnailGenerationState, ProcessingJobSubTaskState thumbnailStorageState) {
        this.retrieveState = retrieveState;
        this.colorExtractionState = colorExtractionState;
        this.metaExtractionState = metaExtractionState;
        this.thumbnailGenerationState = thumbnailGenerationState;
        this.thumbnailStorageState = thumbnailStorageState;
    }

    public ProcessingJobSubTaskState getRetrieveState() {
        return retrieveState;
    }

    public ProcessingJobSubTaskState getColorExtractionState() {
        return colorExtractionState;
    }

    public ProcessingJobSubTaskState getMetaExtractionState() {
        return metaExtractionState;
    }

    public ProcessingJobSubTaskState getThumbnailGenerationState() {
        return thumbnailGenerationState;
    }

    public ProcessingJobSubTaskState getThumbnailStorageState() {
        return thumbnailStorageState;
    }
}
