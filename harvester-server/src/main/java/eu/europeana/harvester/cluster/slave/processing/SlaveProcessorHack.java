package eu.europeana.harvester.cluster.slave.processing;

import eu.europeana.harvester.domain.GenericSubTaskConfiguration;
import eu.europeana.harvester.domain.ProcessingJobSubTask;
import eu.europeana.harvester.domain.ProcessingJobSubTaskType;

/**
 * Created by paul on 07/08/15.
 */
public class SlaveProcessorHack {

    public static ProcessingJobSubTask artificialMetaInfoTask(final ProcessingJobSubTask existingMetaExtractionProcessingTask,
                                                              final ProcessingJobSubTask existingColorExtractionProcessingTask) {
        if (existingMetaExtractionProcessingTask == null && existingColorExtractionProcessingTask != null) {
            return new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION,
                                            new GenericSubTaskConfiguration());
        }
        return existingMetaExtractionProcessingTask;
    }
}
