import akka.event.LoggingAdapter;
import categories.IntegrationTest;
import com.google.common.io.Files;
import eu.europeana.JobCreator.JobCreator;
import eu.europeana.JobCreator.domain.ProcessingJobTuple;
import eu.europeana.harvester.cluster.slave.processing.ProcessingResultTuple;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessor;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailType;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.*;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.*;

/**
 * Created by salexandru on 28.05.2015.
 */

@RunWith (MockitoJUnitRunner.class)
@Category (IntegrationTest.class)
public class HarvesterSlaveIntegrationTest {
    private static String PATH_PREFIX = Paths.get("src/test/resources/").toAbsolutePath().toString() + "/" ;
    private static String PATH_DOWNLOADED = PATH_PREFIX + "downloaded/";
    private static String PATH_COLORMAP = PATH_PREFIX + "colormap.png";

    private SlaveProcessor slaveProcessor;

    @Mock
    private MediaStorageClient mediaStorageClient;

    @Mock
    private LoggingAdapter loggingAdapter;

    @Before
    public void setUp() {
        slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(PATH_COLORMAP),
                                            new ThumbnailGenerator(PATH_COLORMAP),
                                            new ColorExtractor(PATH_COLORMAP),
                                            mediaStorageClient,
                                            loggingAdapter
                                            );
    }


    @Test
    public void test_Task_EdmObject_Image1() throws Exception {
        final String fileName = "image1.jpeg";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";


       final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", fileUrl, null, null, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(
             processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
             PATH_DOWNLOADED  + fileName,
             PATH_DOWNLOADED  + fileName,
             Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
             ResponseType.DISK_STORAGE,
             ""
        );

        assertNull (results.getMediaMetaInfoTuple());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());
        assertEquals (3, results.getGeneratedThumbnails().size());

        final byte[][] thumbnails = new byte[][] {
          Files.toByteArray(new File(PATH_PREFIX + "image1_thumbnail_small.jpeg")),
          Files.toByteArray(new File(PATH_PREFIX + "image1_thumbnail_medium.jpeg")),
          Files.toByteArray(new File(PATH_PREFIX + "image1_thumbnail_large.jpeg")),
        };

        for (final MediaFile thumbnail: results.getGeneratedThumbnails()) {
            assertEquals (PATH_DOWNLOADED + fileName,thumbnail.getOriginalUrl());
            assertEquals (fileName, thumbnail.getName());
            assertEquals ("image/jpeg", thumbnail.getContentType());

            if (!ArrayUtils.isEmpty(results.getImageColorMetaInfo().getColorPalette())) {
                final Map<String, String> colors = new TreeMap<>();
                for (final Map.Entry<String, String> entry: thumbnail.getMetaData().entrySet()) {
                   if (entry.getKey().startsWith("color")) {
                       colors.put(entry.getKey(), entry.getValue());
                   }
                }
                assertArrayEquals(results.getImageColorMetaInfo().getColorPalette(), colors.values().toArray());
            }

            int idx = 0;
            for (final ThumbnailType type: ThumbnailType.values()) {
                if (thumbnail.getSize() == type.getWidth()) {
                    assertArrayEquals (thumbnails[idx], thumbnail.getContent());
                    break;
                }
                ++idx;
            }
            if (idx == ThumbnailType.values().length) {
                fail ("No thumbnail type found for media file generated. Size: " + thumbnail.getSize());
            }
        }
    }

    @Test
    @Ignore
    public void test_Task_EdmHasView() {

    }

    @Test
    @Ignore
    public void test_Task_EdmIsShownBy() {

    }

    @Test
    @Ignore
    public void test_Task_EdmIsShownAt() {

    }
}
