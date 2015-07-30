package eu.europeana.harvester.domain;

import java.io.Serializable;

public class ProcessingJobSubTaskStats implements Serializable {

    private final String retrieveLog;
    private final ProcessingJobSubTaskState retrieveState;
    private final String colorExtractionLog;
    private final ProcessingJobSubTaskState colorExtractionState;
    private final String metaExtractionLog;
    private final ProcessingJobSubTaskState metaExtractionState;
    private final String thumbnailGenerationLog;
    private final ProcessingJobSubTaskState thumbnailGenerationState;
    private final String thumbnailStorageLog;
    private final ProcessingJobSubTaskState thumbnailStorageState;

    public ProcessingJobSubTaskStats() {
        retrieveLog = null;
        retrieveState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        colorExtractionLog = null;
        colorExtractionState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        metaExtractionLog = null;
        metaExtractionState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        thumbnailGenerationLog = null;
        thumbnailGenerationState = ProcessingJobSubTaskState.NEVER_EXECUTED;
        thumbnailStorageLog = null;
        thumbnailStorageState = ProcessingJobSubTaskState.NEVER_EXECUTED;
    }

    public ProcessingJobSubTaskStats(ProcessingJobSubTaskState retrieveState,
                                     ProcessingJobSubTaskState colorExtractionState,
                                     ProcessingJobSubTaskState metaExtractionState,
                                     ProcessingJobSubTaskState thumbnailGenerationState,
                                     ProcessingJobSubTaskState thumbnailStorageState) {

        retrieveLog = null;
        this.retrieveState = retrieveState;
        colorExtractionLog = null;
        this.colorExtractionState = colorExtractionState;
        metaExtractionLog = null;
        this.metaExtractionState = metaExtractionState;
        thumbnailGenerationLog = null;
        this.thumbnailGenerationState = thumbnailGenerationState;
        thumbnailStorageLog = null;
        this.thumbnailStorageState = thumbnailStorageState;
    }

    public ProcessingJobSubTaskStats(String retrieveLog, ProcessingJobSubTaskState retrieveState, String colorExtractionLog, ProcessingJobSubTaskState colorExtractionState, String metaExtractionLog, ProcessingJobSubTaskState metaExtractionState, String thumbnailGenerationLog, ProcessingJobSubTaskState thumbnailGenerationState, String thumbnailStorageLog, ProcessingJobSubTaskState thumbnailStorageState) {
        this.retrieveLog = retrieveLog;
        this.retrieveState = retrieveState;
        this.colorExtractionLog = colorExtractionLog;
        this.colorExtractionState = colorExtractionState;
        this.metaExtractionLog = metaExtractionLog;
        this.metaExtractionState = metaExtractionState;
        this.thumbnailGenerationLog = thumbnailGenerationLog;
        this.thumbnailGenerationState = thumbnailGenerationState;
        this.thumbnailStorageLog = thumbnailStorageLog;
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

    public String getRetrieveLog() {
        return retrieveLog;
    }

    public String getColorExtractionLog() {
        return colorExtractionLog;
    }

    public String getMetaExtractionLog() {
        return metaExtractionLog;
    }

    public String getThumbnailGenerationLog() {
        return thumbnailGenerationLog;
    }

    public String getThumbnailStorageLog() {
        return thumbnailStorageLog;
    }

    public ProcessingJobSubTaskStats withRetrieveState(final ProcessingJobSubTaskState newRetrieveState) {
        return withRetrieveState(newRetrieveState, null);
    }

    public ProcessingJobSubTaskStats withRetrieveState(final ProcessingJobSubTaskState newRetrieveState, final Throwable t) {
        return new ProcessingJobSubTaskStats((t != null) ? t.getMessage() : null,
                newRetrieveState,
                colorExtractionLog,
                colorExtractionState,
                metaExtractionLog,
                metaExtractionState,
                thumbnailGenerationLog,
                thumbnailGenerationState,
                thumbnailStorageLog,
                thumbnailStorageState
        );
    }

    public ProcessingJobSubTaskStats withColorExtractionState(final ProcessingJobSubTaskState newColorExtractionState) {
        return withColorExtractionState(newColorExtractionState, null);
    }

    public ProcessingJobSubTaskStats withColorExtractionState(final ProcessingJobSubTaskState newColorExtractionState, final Throwable t) {
        return new ProcessingJobSubTaskStats(
                retrieveLog,
                retrieveState,
                (t != null) ? t.getMessage() : null,
                newColorExtractionState,
                metaExtractionLog,
                metaExtractionState,
                thumbnailGenerationLog,
                thumbnailGenerationState,
                thumbnailStorageLog,
                thumbnailStorageState
        );
    }

    public ProcessingJobSubTaskStats withMetaExtractionState(final ProcessingJobSubTaskState newMetaExtractionState) {
        return withMetaExtractionState(newMetaExtractionState, null);
    }

    public ProcessingJobSubTaskStats withMetaExtractionState(final ProcessingJobSubTaskState newMetaExtractionState, final Throwable t) {
        return new ProcessingJobSubTaskStats(
                retrieveLog,
                retrieveState,
                colorExtractionLog,
                colorExtractionState,
                (t != null) ? t.getMessage() : null,
                newMetaExtractionState,
                thumbnailGenerationLog,
                thumbnailGenerationState,
                thumbnailStorageLog,
                thumbnailStorageState
        );
    }

    public ProcessingJobSubTaskStats withThumbnailGenerationState(final ProcessingJobSubTaskState newThumbnailGenerationState) {
        return withThumbnailGenerationState(newThumbnailGenerationState, null);
    }

    public ProcessingJobSubTaskStats withThumbnailGenerationState(final ProcessingJobSubTaskState newThumbnailGenerationState, final Throwable t) {
        return new ProcessingJobSubTaskStats(
                retrieveLog,
                retrieveState,
                colorExtractionLog,
                colorExtractionState,
                metaExtractionLog,
                metaExtractionState,
                (t != null) ? t.getMessage() : null,
                newThumbnailGenerationState,
                thumbnailStorageLog,
                thumbnailStorageState
        );
    }

    public ProcessingJobSubTaskStats withThumbnailStorageState(final ProcessingJobSubTaskState newThumbnailStorageState) {
        return new ProcessingJobSubTaskStats(
                                                    retrieveLog,
                                                    retrieveState,
                                                    colorExtractionLog,
                                                    colorExtractionState,
                                                    metaExtractionLog,
                                                    metaExtractionState,
                                                    thumbnailGenerationLog,
                                                    thumbnailGenerationState,
                                                    null,
                                                    newThumbnailStorageState
        );
    }

    public ProcessingJobSubTaskStats withThumbnailStorageState(final ProcessingJobSubTaskState newThumbnailStorageState, final Throwable t) {
        return new ProcessingJobSubTaskStats(
                retrieveLog,
                retrieveState,
                colorExtractionLog,
                colorExtractionState,
                metaExtractionLog,
                metaExtractionState,
                thumbnailGenerationLog,
                thumbnailGenerationState,
                (t != null) ? t.getMessage() : null,
                newThumbnailStorageState
        );
    }


    public static ProcessingJobSubTaskStats withRetrievelSuccess() {
        return new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobSubTaskState.SUCCESS, null);
    }
}
