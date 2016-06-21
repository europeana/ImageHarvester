package eu.europeana.harvester.cluster.slave.processing;

import akka.event.LoggingAdapter;
import com.google.common.io.Files;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailImageGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.filesystem.FileSystemMediaStorageClientImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import gr.ntua.image.mediachecker.MediaChecker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static eu.europeana.harvester.TestUtils.*;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@RunWith (MockitoJUnitRunner.class)
public class SlaveProcessorTest {
    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }

        @Override
        protected  void finished(Description description) {
            System.out.println("Stopping test: " + description.getMethodName());
        }
    };

    @Mock
    private LoggingAdapter loggingAdapter;

    private MediaStorageClient mediaStorageClient;

    private ProcessingJobTaskDocumentReference taskDocumentReference;

    private static final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    private  List<ProcessingJobSubTask> subTasks;
    private SlaveProcessor slaveProcessor;

    private static final ReferenceOwner owner = new ReferenceOwner("", "", "", "");

    private static final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION, null);
    private static final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null);
    private static final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(200, 200)));
    private static final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(400, 400)));

    private static final Throwable exception = new Exception("hahaha");

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(new File(PATH_DOWNLOADED));
    }

    @Before
    public void setUp() throws Exception {
        subTasks = new ArrayList<>();
        FileUtils.forceMkdir(new File(PATH_DOWNLOADED));
        mediaStorageClient = new FileSystemMediaStorageClientImpl(PATH_DOWNLOADED);
        slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(PATH_COLORMAP), new ColorExtractor(PATH_COLORMAP), mediaStorageClient, PATH_COLORMAP);

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
                null,new ReferenceOwner("unknown","unknwon","unknown"));

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        assertEquals(RetrievingState.COMPLETED, response.getState());
    }

    private void checkThumbnails (final String imageName, final Collection<MediaFile> genThumbnails, final String[] colorPalette) throws IOException {
        final Map<ThumbnailType, byte[]> thumbnails = new HashMap<>();
        thumbnails.put(ThumbnailType.MEDIUM,
                Image1.equals(imageName) ? filesInBytes.get(Image1ThumbnailMedium) : filesInBytes
                        .get(Image2ThumbnailMedium));
        thumbnails.put(ThumbnailType.LARGE,
                Image1.equals(imageName) ? filesInBytes.get(Image1ThumbnailLarge) : filesInBytes
                        .get(Image2ThumbnailLarge));

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


            boolean ok = false;
            for (final ThumbnailType type: ThumbnailType.values()) {
                if (thumbnail.getSize() == type.getWidth()) {
                    final byte[] first = thumbnails.get(type);
                    final byte[] second = thumbnail.getContent();
                    /* Because of the way compression for images is done two successive compressions are never guaranteed to be identical */
                    assertTrue((first.length / second.length > 0.95) || (second.length / first.length > 0.95));
                    ok = true;
                    break;
                }
            }
            if (!ok) {
                fail("No thumbnail type found for media file generated. Size: " + thumbnail.getSize());
            }
        }
    }

    @Test
    public void test_AllTasks_Image() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference,
                PATH_DOWNLOADED + Image1,
                fileUrl,
                filesInBytes.get(Image1),
                ResponseType.DISK_STORAGE,
                owner);

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNotNull(results.getMediaMetaInfoTuple().getImageMetaInfo().getColorPalette());
        assertNotNull(results.getGeneratedThumbnails());
        assertTrue(new File(PATH_DOWNLOADED + Image1).exists());
        assertEquals(2, results.getGeneratedThumbnails().size());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + Image1, PATH_COLORMAP).getPalette(),
                results.getMediaMetaInfoTuple().getImageMetaInfo().getColorPalette());

        checkThumbnails(Image1, results.getGeneratedThumbnails(), results.getMediaMetaInfoTuple().getImageMetaInfo().getColorPalette());

        final ImageMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + Image1).getImageMetaInfo();
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getImageMetaInfo()));
    }

    @Test
    public void test_Task_ColorExtraction_ThumbnailGeneration_Image() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference,
                PATH_DOWNLOADED + Image1,
                fileUrl,
                filesInBytes.get(Image1),
                ResponseType.DISK_STORAGE,
                owner);

        assertNotNull(results.getMediaMetaInfoTuple().getImageMetaInfo().getColorPalette());
        assertFalse(results.getGeneratedThumbnails().isEmpty());
        assertTrue(new File(PATH_DOWNLOADED + Image1).exists());
        assertEquals(2, results.getGeneratedThumbnails().size());

    }

    @Test
    public void test_AllTask_Audio() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Audio1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Audio1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference, PATH_DOWNLOADED + Audio1,
                fileUrl,
                Files.toByteArray(new File(PATH_DOWNLOADED + Audio1)),
                ResponseType.DISK_STORAGE,
                owner);

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertTrue(new File(PATH_DOWNLOADED + Audio1).exists());

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
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + Video1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference, PATH_DOWNLOADED + Video1,
                fileUrl,
                Files.toByteArray(new File(PATH_DOWNLOADED +
                        Video1)),
                ResponseType.DISK_STORAGE, owner);

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNull(results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertTrue(new File(PATH_DOWNLOADED + Video1).exists());

        assertTrue(null == results.getGeneratedThumbnails() || ArrayUtils.isEmpty(results.getGeneratedThumbnails()
                .toArray()));
        final VideoMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + Video1).getVideoMetaInfo();
        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getVideoMetaInfo()));
    }

    @Test
    public void test_AllTask_Text() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + PDF1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        downloadFile(fileUrl, PATH_DOWNLOADED + PDF1);

        final ProcessingResultTuple results = slaveProcessor.process(taskDocumentReference, PATH_DOWNLOADED + PDF1,
                fileUrl,
                Files.toByteArray(new File(PATH_DOWNLOADED +
                        PDF1)),
                ResponseType.DISK_STORAGE,
                new ReferenceOwner("unknown", "unknwon",
                        "unknown"));

        assertNotNull(results.getMediaMetaInfoTuple());
        assertNotNull(results.getMediaMetaInfoTuple().getTextMetaInfo());
        assertNull(results.getMediaMetaInfoTuple().getImageMetaInfo());
        assertTrue(new File(PATH_DOWNLOADED + PDF1).exists());

        assertTrue(null == results.getGeneratedThumbnails() || ArrayUtils.isEmpty(results.getGeneratedThumbnails()
                .toArray()));
//        final TextMetaInfo metaInfo = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + PDF1).getTextMetaInfo();
//        assertTrue(EqualsBuilder.reflectionEquals(metaInfo, results.getMediaMetaInfoTuple().getTextMetaInfo()));
    }

    @Test
    public void test_MetaInfoExtractionFails() throws Exception{
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);
        ThumbnailImageGenerator thumbnailImageGeneratorFail = mock(ThumbnailImageGenerator.class);

        doThrow(exception).when(mediaMetaInfoExtractorFail).extract(anyString());
        doReturn(null).when(colorExtractorFail).colorExtraction(anyString());
        doReturn(PATH_COLORMAP).when(thumbnailImageGeneratorFail).getColorMapPath();
        doReturn(null).when(thumbnailImageGeneratorFail).createMediaFileWithThumbnail(anyInt(), anyInt(), anyString(),
                anyString(),
                any(new byte[]{}.getClass()),
                anyString());

        SlaveProcessor slaveProcessorAlwaysFail = new SlaveProcessor(mediaMetaInfoExtractorFail, colorExtractorFail, mediaStorageClientFail, PATH_COLORMAP);

        ProcessingResultTuple e = slaveProcessorAlwaysFail.process(taskDocumentReference, PATH_DOWNLOADED + Image1, fileUrl,
                new byte[]{0}, ResponseType.DISK_STORAGE,
                new ReferenceOwner("", "", "", "")) ;

        assertEquals (ProcessingJobSubTaskState.ERROR, e.getProcessingJobSubTaskStats().getMetaExtractionState());
        assertEquals (exception.toString(), e.getProcessingJobSubTaskStats().getMetaExtractionLog());

    }

    @Test
    public void test_WhenColorExtractionIsPresentAndMetaInfoExtractionNotThenTheMetaInfoIsInsertedArtificiallyAsAHack() throws Exception{
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);
        ThumbnailImageGenerator thumbnailImageGeneratorFail = mock(ThumbnailImageGenerator.class);

        doThrow(exception).when(mediaMetaInfoExtractorFail).extract(anyString());
        doReturn(null).when(colorExtractorFail).colorExtraction(anyString());
        doReturn(PATH_COLORMAP).when(thumbnailImageGeneratorFail).getColorMapPath();
        doReturn(null).when(thumbnailImageGeneratorFail).createMediaFileWithThumbnail(anyInt(), anyInt(), anyString(),
                anyString(),
                any(new byte[]{}.getClass()),
                anyString());

        SlaveProcessor slaveProcessorAlwaysFail = new SlaveProcessor(mediaMetaInfoExtractorFail, colorExtractorFail, mediaStorageClientFail, PATH_COLORMAP);

        ProcessingResultTuple e = slaveProcessorAlwaysFail.process(taskDocumentReference, PATH_DOWNLOADED + Image1, fileUrl,
                new byte[]{0}, ResponseType.DISK_STORAGE,
                new ReferenceOwner("", "", "", "")) ;

        assertEquals (ProcessingJobSubTaskState.ERROR, e.getProcessingJobSubTaskStats().getMetaExtractionState());
        assertEquals (exception.toString(), e.getProcessingJobSubTaskStats().getMetaExtractionLog());

    }

    @Test
    public void test_ColorExtractionFails() throws Exception{
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);
        ThumbnailImageGenerator thumbnailImageGeneratorFail = mock(ThumbnailImageGenerator.class);

        doReturn(null).when(mediaMetaInfoExtractorFail).extract(anyString());
        doThrow(new IOException(exception)).when(colorExtractorFail).colorExtraction(anyString());
        doReturn(PATH_COLORMAP).when(thumbnailImageGeneratorFail).getColorMapPath();
        doReturn(null).when(thumbnailImageGeneratorFail).createMediaFileWithThumbnail(anyInt(), anyInt(), anyString(),
                anyString(),
                any(new byte[]{}.getClass()),
                anyString());

        SlaveProcessor slaveProcessorAlwaysFail = new SlaveProcessor(mediaMetaInfoExtractorFail, colorExtractorFail, mediaStorageClientFail, PATH_COLORMAP);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);
        ProcessingResultTuple e =            slaveProcessorAlwaysFail.process(taskDocumentReference,
                PATH_DOWNLOADED + Image1, fileUrl,
                new byte[]{0}, ResponseType.DISK_STORAGE,
                new ReferenceOwner("", "", "", "")) ;
        assertEquals (ProcessingJobSubTaskState.ERROR, e.getProcessingJobSubTaskStats().getColorExtractionState());
        assertNotNull(e.getProcessingJobSubTaskStats().getColorExtractionLog());
    }

    @Test
    public void test_ThumbnailGeneratorNeverExecutesWhenMetaInfoExtractionAndColorFails() throws Exception{
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);
        ThumbnailImageGenerator thumbnailImageGeneratorFail = mock(ThumbnailImageGenerator.class);

        doReturn(null).when(mediaMetaInfoExtractorFail).extract(anyString());
        doReturn(null).when(colorExtractorFail).colorExtraction(anyString());
        doReturn(PATH_COLORMAP).when(thumbnailImageGeneratorFail).getColorMapPath();
        doThrow(exception).when(thumbnailImageGeneratorFail).createMediaFileWithThumbnail(anyInt(), anyInt(), anyString(),
                anyString(),
                any(new byte[]{}.getClass()),
                anyString());

        SlaveProcessor slaveProcessorAlwaysFail = new SlaveProcessor(mediaMetaInfoExtractorFail, colorExtractorFail, mediaStorageClientFail, PATH_COLORMAP);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);
        ProcessingResultTuple e = slaveProcessorAlwaysFail.process(taskDocumentReference,
                PATH_DOWNLOADED + Image1, fileUrl,
                new byte[]{0}, ResponseType.DISK_STORAGE,
                new ReferenceOwner("", "", "", "")) ;
        assertEquals(ProcessingJobSubTaskState.NEVER_EXECUTED, e.getProcessingJobSubTaskStats().getThumbnailGenerationState());
        assertEquals (null, e.getProcessingJobSubTaskStats().getThumbnailGenerationLog());
    }

    @Test
    public void test_AllExtractionReturnNull_MediaStoragesThrows() throws Exception {
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);

        doReturn(null).when(mediaMetaInfoExtractorFail).extract(anyString());
        doReturn(null).when(colorExtractorFail).colorExtraction(anyString());

        doThrow(new RuntimeException(exception)).when(mediaStorageClientFail).createOrModify(any(MediaFile.class));

        SlaveProcessor slaveProcessorAlwaysFail = new SlaveProcessor(mediaMetaInfoExtractorFail, colorExtractorFail, mediaStorageClientFail, PATH_COLORMAP);

        downloadFile(fileUrl, PATH_DOWNLOADED + Image1);
        ProcessingResultTuple e = slaveProcessorAlwaysFail.process(taskDocumentReference, PATH_DOWNLOADED + Image1,
                fileUrl,
                Files.toByteArray(new File(PATH_DOWNLOADED + Image1)),
                ResponseType.DISK_STORAGE,
                new ReferenceOwner("", "", "", "")) ;

        assertEquals (ProcessingJobSubTaskState.FAILED, e.getProcessingJobSubTaskStats().getMetaExtractionState());
        assertEquals (ProcessingJobSubTaskState.FAILED, e.getProcessingJobSubTaskStats().getColorExtractionState());
        assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, e.getProcessingJobSubTaskStats().getThumbnailGenerationState());
        assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, e.getProcessingJobSubTaskStats().getThumbnailStorageState());
    }

    @Test
    public void test_AllExtractionReturnsNull() throws Exception{
        final String fileUrl = GitHubUrl_PREFIX + Image1;

        subTasks.add(metaInfoExtractionSubTask);
        subTasks.add(colorExtractionSubTask);
        subTasks.add(mediumThumbnailExtractionSubTask);
        subTasks.add(largeThumbnailExtractionSubTask);

        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);
        ThumbnailImageGenerator thumbnailImageGeneratorFail = mock(ThumbnailImageGenerator.class);

        doReturn(null).when(mediaMetaInfoExtractorFail).extract(anyString());
        doReturn(null).when(colorExtractorFail).colorExtraction(anyString());
        doReturn(PATH_COLORMAP).when(thumbnailImageGeneratorFail).getColorMapPath();
        doReturn(null).when(thumbnailImageGeneratorFail).createMediaFileWithThumbnail(anyInt(), anyInt(), anyString(),
                anyString(),
                any(new byte[]{}.getClass()),
                anyString());

        SlaveProcessor slaveProcessorAlwaysFail = new SlaveProcessor(mediaMetaInfoExtractorFail, colorExtractorFail, mediaStorageClientFail, PATH_COLORMAP);

        ProcessingResultTuple tuple =    slaveProcessorAlwaysFail.process(taskDocumentReference, PATH_DOWNLOADED + Image1,
                fileUrl, new byte[]{0}, ResponseType.DISK_STORAGE,
                new ReferenceOwner("", "", "", "")) ;

        assertEquals(ProcessingJobSubTaskState.FAILED, tuple.getProcessingJobSubTaskStats().getMetaExtractionState());
        assertEquals(ProcessingJobSubTaskState.FAILED, tuple.getProcessingJobSubTaskStats().getColorExtractionState());
        assertEquals(ProcessingJobSubTaskState.NEVER_EXECUTED, tuple.getProcessingJobSubTaskStats().getThumbnailGenerationState());
        assertEquals(ProcessingJobSubTaskState.NEVER_EXECUTED, tuple.getProcessingJobSubTaskStats().getThumbnailStorageState());
    }
}
