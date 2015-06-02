package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.*;

import java.util.Arrays;
import java.util.List;

/**
 * Builder for various types of sub tasks.
 */
public class SubTaskBuilder {

    /**
     * Creates the thumbnail generation sub tasks.
     * @return
     */
    public static final List<ProcessingJobSubTask> thumbnailGeneration() {
        return Arrays.asList(
                new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(ThumbnailType.SMALL.getWidth(), ThumbnailType.SMALL.getHeight()))),
                new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(ThumbnailType.MEDIUM.getWidth(), ThumbnailType.MEDIUM.getHeight()))),
                new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(ThumbnailType.LARGE.getWidth(), ThumbnailType.LARGE.getHeight())))
        );
    }

    /**
     * Creates the meta extraction sub tasks.
     * @return
     */
    public static final List<ProcessingJobSubTask> metaExtraction() {
        return Arrays.asList(
                new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null));
    }

    /**
     * Creates the colour extraction sub tasks.
     * @return
     */
    public static final List<ProcessingJobSubTask> colourExtraction() {
        return Arrays.asList(
                new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION, null));
    }
}
