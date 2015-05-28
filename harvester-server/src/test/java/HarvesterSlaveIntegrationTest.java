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
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.ResponseType;
import gr.ntua.image.mediachecker.MediaChecker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

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

    private void checkThumbnails(final String imageName, final Collection<MediaFile> genThumbnails, final String[] colorPalette) throws IOException {
        final String fileName = imageName + ".jpeg";
        final byte[][] thumbnails = new byte[][] {
            Files.toByteArray(new File(PATH_PREFIX + imageName + "_thumbnail_small.jpeg")),
            Files.toByteArray(new File(PATH_PREFIX + imageName + "_thumbnail_medium.jpeg")),
            Files.toByteArray(new File(PATH_PREFIX + imageName + "_thumbnail_large.jpeg")),
        };

        for (final MediaFile thumbnail: genThumbnails) {
            assertEquals (PATH_DOWNLOADED + fileName,thumbnail.getOriginalUrl());
            assertEquals (fileName, thumbnail.getName());
            assertEquals ("image/jpeg", thumbnail.getContentType());

            if (!ArrayUtils.isEmpty(colorPalette)) {
                final Map<String, String> colors = new TreeMap<>();
                for (final Map.Entry<String, String> entry: thumbnail.getMetaData().entrySet()) {
                    if (entry.getKey().startsWith("color")) {
                        colors.put(entry.getKey(), entry.getValue());
                    }
                }
                assertArrayEquals(colorPalette, colors.values().toArray());
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
        assertNotNull(results.getImageColorMetaInfo());
        assertNotNull(results.getGeneratedThumbnails());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());
        assertEquals (3, results.getGeneratedThumbnails().size());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                                 results.getImageColorMetaInfo().getColorPalette()
        );

        checkThumbnails("image1", results.getGeneratedThumbnails(), results.getImageColorMetaInfo().getColorPalette());
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Image1() throws Exception {
        final String fileName = "image1.jpeg";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", fileUrl, null, null, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                     PATH_DOWNLOADED  + fileName,
                                                                     PATH_DOWNLOADED  + fileName,
                                                                     Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                     ResponseType.DISK_STORAGE,
                                                                     ""
                                                                    );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNotNull(results.getImageColorMetaInfo());
        assertNotNull(results.getGeneratedThumbnails());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());
        assertEquals (3, results.getGeneratedThumbnails().size());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                                 results.getImageColorMetaInfo().getColorPalette()
        );

        checkThumbnails("image1", results.getGeneratedThumbnails(), results.getImageColorMetaInfo().getColorPalette());

        final ImageMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getImageMetaInfo();

        assertNotNull("Image meta info must not be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getImageMetaInfo()));
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Image2() throws Exception {
        final String fileName = "image2.jpeg";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", fileUrl, null, null, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNotNull(results.getImageColorMetaInfo());
        assertNotNull(results.getGeneratedThumbnails());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());
        assertEquals (3, results.getGeneratedThumbnails().size());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                                 results.getImageColorMetaInfo().getColorPalette()
        );

        checkThumbnails("image2", results.getGeneratedThumbnails(), results.getImageColorMetaInfo().getColorPalette());

        final ImageMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getImageMetaInfo();

        assertNotNull("Image meta info must not be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getImageMetaInfo()));
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Audio1() throws Exception {
        final String fileName = "audio1.mp3";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", null, null, fileUrl, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertTrue(results.getGeneratedThumbnails().isEmpty());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());

        final AudioMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getAudioMetaInfo();

        assertNotNull(results.getMediaMetaInfoTuple().getAudioMetaInfo());
        assertNull("Image meta info should be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getAudioMetaInfo()));
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Audio2() throws Exception {
        final String fileName = "audio2.mp3";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", null, null, fileUrl, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertTrue(results.getGeneratedThumbnails().isEmpty());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());

        final AudioMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getAudioMetaInfo();

        assertNotNull(results.getMediaMetaInfoTuple().getAudioMetaInfo());
        assertNull("Image meta info should be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getAudioMetaInfo()));
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Video1() throws Exception {
        final String fileName = "video1.mpg";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", null, null, fileUrl, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertTrue(results.getGeneratedThumbnails().isEmpty());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());

        final VideoMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getVideoMetaInfo();

        assertNotNull(results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("Image meta info should be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getAudioMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getVideoMetaInfo()));

    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Video2() throws Exception {
        final String fileName = "video2.mpg";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", null, null, fileUrl, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertTrue(results.getGeneratedThumbnails().isEmpty());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());

        final VideoMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getVideoMetaInfo();

        assertNotNull(results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("Image meta info should be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getAudioMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getVideoMetaInfo()));
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Text1() throws Exception {
        final String fileName = "text1.pdf";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", null, null, fileUrl, null, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertTrue(results.getGeneratedThumbnails().isEmpty());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());

        final TextMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getTextMetaInfo();

        assertNotNull(results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertNull("Image meta info should be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getTextMetaInfo()));
    }

    @Test @Ignore
    public void test_Task_EdmIsShownBy_Text2() throws Exception {
        final String fileName = "text2.pdf";
        final String fileUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/" + fileName;


        final List<ProcessingJobTuple> processingJobTupels = JobCreator.createJobs("", "", "", null, null, fileUrl, null, null);

        assertEquals(1, processingJobTupels.size());
        assertEquals(1, processingJobTupels.get(0).getProcessingJob().getTasks().size());


        FileUtils.copyURLToFile(new URL(fileUrl), new File(PATH_DOWNLOADED + fileName));

        final ProcessingResultTuple results = slaveProcessor.process(processingJobTupels.get(0).getProcessingJob().getTasks().get(0),
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            PATH_DOWNLOADED  + fileName,
                                                                            Files.toByteArray(new File(PATH_DOWNLOADED + fileName)),
                                                                            ResponseType.DISK_STORAGE,
                                                                            ""
        );

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertTrue(results.getGeneratedThumbnails().isEmpty());
        assertFalse(new File(PATH_DOWNLOADED + fileName).exists());

        final TextMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName).getTextMetaInfo();

        assertNotNull(results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertNull("Image meta info should be null!", results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getTextMetaInfo()));
    }
}
