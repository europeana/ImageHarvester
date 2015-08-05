package eu.europeana.harvester.cluster.slave;

import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.domain.ProcessingJobSubTaskState;
import eu.europeana.harvester.domain.ProcessingJobSubTaskStats;
import eu.europeana.harvester.domain.ProcessingState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DoneProcessingTests {
    @Test
    public void canComputeStateCombiningProcessingStateAndSubTasksStates() throws Exception {

        assertEquals(ProcessingState.ERROR,
                DoneProcessing.computeProcessingStateIncludingTheSubTaskState(ProcessingState.SUCCESS,
                        new ProcessingJobSubTaskStats(
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* retrieveState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* colorExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* metaExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailGenerationState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailStorageState */)));

        assertEquals(ProcessingState.SUCCESS,
                DoneProcessing.computeProcessingStateIncludingTheSubTaskState(ProcessingState.SUCCESS,
                        new ProcessingJobSubTaskStats(
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* retrieveState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* colorExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* metaExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailGenerationState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailStorageState */)));

        assertEquals(ProcessingState.ERROR,
                DoneProcessing.computeProcessingStateIncludingTheSubTaskState(ProcessingState.SUCCESS,
                        new ProcessingJobSubTaskStats(
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* retrieveState */,
                                null,
                                ProcessingJobSubTaskState.ERROR /* colorExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* metaExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailGenerationState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailStorageState */)));

        assertEquals(ProcessingState.ERROR,
                DoneProcessing.computeProcessingStateIncludingTheSubTaskState(ProcessingState.SUCCESS,
                        new ProcessingJobSubTaskStats(
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* retrieveState */,
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* colorExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* metaExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailGenerationState */,
                                null,
                                ProcessingJobSubTaskState.FAILED /* thumbnailStorageState */)));

        assertEquals(ProcessingState.SUCCESS,
                DoneProcessing.computeProcessingStateIncludingTheSubTaskState(ProcessingState.SUCCESS,
                        new ProcessingJobSubTaskStats(
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* retrieveState */,
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* colorExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* metaExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailGenerationState */,
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* thumbnailStorageState */)));

        assertEquals(ProcessingState.ERROR,
                DoneProcessing.computeProcessingStateIncludingTheSubTaskState(ProcessingState.ERROR,
                        new ProcessingJobSubTaskStats(
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* retrieveState */,
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* colorExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* metaExtractionState */,
                                null,
                                ProcessingJobSubTaskState.NEVER_EXECUTED /* thumbnailGenerationState */,
                                null,
                                ProcessingJobSubTaskState.SUCCESS /* thumbnailStorageState */)));

    }

}