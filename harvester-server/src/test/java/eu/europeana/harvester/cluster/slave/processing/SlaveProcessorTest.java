package eu.europeana.harvester.cluster.slave.processing;

import akka.event.LoggingAdapter;
import com.google.common.io.Files;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.filesystem.FileSystemMediaStorageClientImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import eu.europeana.harvester.httpclient.response.ResponseType;
import gr.ntua.image.mediachecker.MediaChecker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static  eu.europeana.harvester.TestUtils.*;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith (MockitoJUnitRunner.class)
public class SlaveProcessorTest {

    @Mock
    private LoggingAdapter loggingAdapter;

    private MediaStorageClient mediaStorageClient;

    private ProcessingJobTaskDocumentReference taskDocumentReference;

    private static final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    private  List<ProcessingJobSubTask> subTasks;
    private SlaveProcessor slaveProcessor;

    private static final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION, null);
    private static final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null);
    private static final ProcessingJobSubTask smallThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(180, 180)));
    private static final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(200, 200)));
    private static final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(400, 400)));

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(PATH_DOWNLOADED));
    }

    @Before
    public void setUp() throws IOException {
        subTasks = new ArrayList<>();
        FileUtils.forceMkdir(new File(PATH_DOWNLOADED));
        mediaStorageClient = new FileSystemMediaStorageClientImpl(PATH_DOWNLOADED);
        slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(PATH_COLORMAP),
                                            new ThumbnailGenerator(PATH_COLORMAP),
                                            new ColorExtractor(PATH_COLORMAP),
                                            mediaStorageClient
                                          );
        taskDocumentReference = new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                                                           "source-reference-1", subTasks);
    }

    private void downloadFile (final String url, final String pathToStore) throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader();

        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathToStore);

        final RetrieveUrl task = new RetrieveUrl(
                                                 url,
                                                 new ProcessingJobLimits(),
                                                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                                 "jobid-1",
                                                 "referenceid-1", Collections.<String, String>emptyMap(),
                                                 taskDocumentReference,
                                                 null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.COMPLETED, response.getState());
    }

    private void checkThumbnails (final String imageName, final Collection<MediaFile> genThumbnails, final String[] colorPalette) throws IOException {
        final byte[][] thumbnails = new byte[][] {
             Image1.equals(imageName) ? imagesInBytes.get(Image1ThumbnailSmall) : imagesInBytes.get(Image2ThumbnailSmall),
             Image1.equals(imageName) ? imagesInBytes.get(Image1ThumbnailMedium) : imagesInBytes.get(Image2ThumbnailMedium),
             Image1.equals(imageName) ? imagesInBytes.get(Image1ThumbnailLarge) : imagesInBytes.get(Image2ThumbnailLarge)
        };

        for (final MediaFile thumbnail: genThumbnails) {
            assertEquals (GitHubUrl_PREFIX + imageName,thumbnail.getOriginalUrl());
            assertEquals (imageName, thumbnail.getName());
            assertTrue (IMAGE_MIMETYPE.equals(thumbnail.getContentType()));

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
                    assertArrayEquals(thumbnails[idx], thumbnail.getContent());
                    break;
                }
                ++idx;
            }
            if (idx == ThumbnailType.values().length) {
                fail("No thumbnail type found for media file generated. Size: " + thumbnail.getSize());
            }
        }
    }

    @Test
    public void test_AllTasks_Image() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Image1;


        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(smallThumbnailExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference,
                                                                    PATH_DOWNLOADED + Image1,
                                                                    fileUrl,
                                                                    imagesInBytes.get(Image1),
                                                                    ResponseType.DISK_STORAGE,
                                                                    null);

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNotNull(results.getImageColorMetaInfo());
        assertNotNull(results.getGeneratedThumbnails());
        assertFalse(new File(PATH_DOWNLOADED + Image1).exists());
        assertEquals(3, results.getGeneratedThumbnails().size());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + Image1, PATH_COLORMAP).getPalette(),
                          results.getImageColorMetaInfo().getColorPalette());

        checkThumbnails(Image1, results.getGeneratedThumbnails(), results.getImageColorMetaInfo().getColorPalette());

        final ImageMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + Image1).getImageMetaInfo();
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getImageMetaInfo()));
    }

    @Test
    public void test_Task_ColorExtraction_ThumbnailGeneration_Image() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(colorExtractionSubTask);
        subTasks.add(smallThumbnailExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference,
                                                                     PATH_DOWNLOADED + Image1,
                                                                     fileUrl,
                                                                     imagesInBytes.get(Image1),
                                                                     ResponseType.DISK_STORAGE,
                                                                     null);

        assertNull(results.getMediaMetaInfoTuple());
        assertNotNull(results.getImageColorMetaInfo());
        assertNotNull(results.getGeneratedThumbnails());
        assertFalse(new File(PATH_DOWNLOADED + Image1).exists());
        assertEquals(3, results.getGeneratedThumbnails().size());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + Image1, PATH_COLORMAP).getPalette(),
                          results.getImageColorMetaInfo().getColorPalette());

        checkThumbnails(Image1, results.getGeneratedThumbnails(), results.getImageColorMetaInfo().getColorPalette());
    }

    @Test
    public void test_AllTask_Audio() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Audio1;


        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(smallThumbnailExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Audio1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference, PATH_DOWNLOADED + Audio1,
                                                                     fileUrl,
                                                                     Files.toByteArray(new File(PATH_DOWNLOADED + Audio1)),
                                                                     ResponseType.DISK_STORAGE,
                                                                     null);

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertFalse(new File(PATH_DOWNLOADED + Audio1).exists());

        assertTrue(null == results.getGeneratedThumbnails() || ArrayUtils.isEmpty(results.getGeneratedThumbnails()
                                                                                         .toArray()));

        final AudioMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + Audio1).getAudioMetaInfo();
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getAudioMetaInfo()));
    }

    @Test
    public void test_AllTask_Video() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Video1;


        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(smallThumbnailExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Video1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference, PATH_DOWNLOADED + Video1,
                                                                     fileUrl,
                                                                     Files.toByteArray(new File(PATH_DOWNLOADED + Video1)),
                                                                     ResponseType.DISK_STORAGE,
                                                                     null);

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertFalse(new File(PATH_DOWNLOADED + Video1).exists());

        assertTrue(null == results.getGeneratedThumbnails() || ArrayUtils.isEmpty(results.getGeneratedThumbnails()
                                                                                         .toArray()));
        final VideoMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + Video1).getVideoMetaInfo();
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getVideoMetaInfo()));
    }

    @Test
    public void test_AllTask_Text() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Text2;


        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(smallThumbnailExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Text2);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference, PATH_DOWNLOADED + Text2,
                                                                     fileUrl,
                                                                     Files.toByteArray(new File(PATH_DOWNLOADED + Text2)),
                                                                     ResponseType.DISK_STORAGE,
                                                                     "");

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getImageColorMetaInfo());
        assertFalse(new File(PATH_DOWNLOADED + Text2).exists());

        assertTrue(null == results.getGeneratedThumbnails() || ArrayUtils.isEmpty(results.getGeneratedThumbnails()
                                                                                         .toArray()));

        final TextMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + Text2).getTextMetaInfo();
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getTextMetaInfo()));
    }

}
