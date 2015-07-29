package eu.europeana.harvester.domain;

import com.sun.org.apache.xalan.internal.xslt.*;

public class ProcessingJobSubTaskStats {

    private final ProcessingJobSubTaskState retrieveState;
    private final ProcessingJobSubTaskState colorExtractionState;
    private final ProcessingJobSubTaskState metaExtractionState;
    private final ProcessingJobSubTaskState thumbnailGenerationState;
    private final ProcessingJobSubTaskState thumbnailStorageState;

    public ProcessingJobSubTaskStats() {
        retrieveState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        colorExtractionState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        metaExtractionState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        thumbnailGenerationState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        thumbnailStorageState = ProcessingJobSubTaskState.NEVER_EXECUTED;
    }

    public ProcessingJobSubTaskStats(ProcessingJobSubTaskState retrieveState,
                                     ProcessingJobSubTaskState colorExtractionState,
                                     ProcessingJobSubTaskState metaExtractionState,
                                     ProcessingJobSubTaskState thumbnailGenerationState,
                                     ProcessingJobSubTaskState thumbnailStorageState) {

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

    public ProcessingJobSubTaskStats withRetrieveState (ProcessingJobSubTaskState retrieveState) {
        return new ProcessingJobSubTaskStats(retrieveState,
                                             colorExtractionState,
                                             metaExtractionState,
                                             thumbnailGenerationState,
                                             thumbnailStorageState
                                             );
    }

    public ProcessingJobSubTaskStats withColorExtractionState (ProcessingJobSubTaskState colorExtractionState) {
        return new ProcessingJobSubTaskStats(retrieveState,
                                             colorExtractionState,
                                             metaExtractionState,
                                             thumbnailGenerationState,
                                             thumbnailStorageState
                                             );
    }

    public ProcessingJobSubTaskStats withMetaExtractionState (ProcessingJobSubTaskState metaExtractionState) {
        return new ProcessingJobSubTaskStats(retrieveState,
                                             colorExtractionState,
                                             metaExtractionState,
                                             thumbnailGenerationState,
                                             thumbnailStorageState
                                             );
    }

    public ProcessingJobSubTaskStats withThumbnailGenerationState (ProcessingJobSubTaskState thumbnailGenerationState) {
        return new ProcessingJobSubTaskStats(retrieveState,
                                             colorExtractionState,
                                             metaExtractionState,
                                             thumbnailGenerationState,
                                             thumbnailStorageState
                                             );
    }


    public ProcessingJobSubTaskStats withThumbnailStorageState (ProcessingJobSubTaskState thumbnailStorageState) {
        return new ProcessingJobSubTaskStats(retrieveState,
                                             colorExtractionState,
                                             metaExtractionState,
                                             thumbnailGenerationState,
                                             thumbnailStorageState
                                             );
    }

    public static ProcessingJobSubTaskStats withProcessingTasksError () {
         return new ProcessingJobSubTaskStats(
                                             ProcessingJobSubTaskState.SUCCESS,
                                             ProcessingJobSubTaskState.ERROR,
                                             ProcessingJobSubTaskState.ERROR,
                                             ProcessingJobSubTaskState.ERROR,
                                             ProcessingJobSubTaskState.ERROR
                                             );


    }

    public static ProcessingJobSubTaskStats withRetrievelSuccess () {
        return new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.SUCCESS);
    }
}
