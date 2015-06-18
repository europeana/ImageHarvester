package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.ProcessingJobSubTask;
import eu.europeana.harvester.domain.ProcessingJobSubTaskType;
import eu.europeana.harvester.domain.ThumbnailType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by salexandru on 02.06.2015.
 */
public class SubTaskBuilderTest {

    @Test
    public void test_thumbnailGeneration() {
        final List<ProcessingJobSubTask> subTasks = SubTaskBuilder.thumbnailGeneration();

        assertEquals (2, subTasks.size());
        assertEquals (ProcessingJobSubTaskType.GENERATE_THUMBNAIL, subTasks.get(0).getTaskType());
        assertEquals (ProcessingJobSubTaskType.GENERATE_THUMBNAIL, subTasks.get(1).getTaskType());
        assertEquals ((Integer)ThumbnailType.MEDIUM.getHeight(), subTasks.get(0).getConfig().getThumbnailConfig().getHeight());
        assertEquals ((Integer)ThumbnailType.MEDIUM.getWidth(), subTasks.get(0).getConfig().getThumbnailConfig().getWidth());
        assertEquals ((Integer)ThumbnailType.LARGE.getHeight(), subTasks.get(1).getConfig().getThumbnailConfig().getHeight());
        assertEquals ((Integer)ThumbnailType.LARGE.getWidth(), subTasks.get(1).getConfig().getThumbnailConfig().getWidth());
    }

    @Test
    public void test_colorExtraction() {
        final List<ProcessingJobSubTask> subTasks = SubTaskBuilder.colourExtraction();

        assertEquals (1, subTasks.size());
        assertEquals (ProcessingJobSubTaskType.COLOR_EXTRACTION, subTasks.get(0).getTaskType());
        assertNull   (subTasks.get(0).getConfig());
    }

    @Test
    public void test_metaExtraction() {
        final List<ProcessingJobSubTask> subTasks = SubTaskBuilder.metaExtraction();

        assertEquals (1, subTasks.size());
        assertEquals (ProcessingJobSubTaskType.META_EXTRACTION, subTasks.get(0).getTaskType());
        assertNull   (subTasks.get(0).getConfig());
    }
}
