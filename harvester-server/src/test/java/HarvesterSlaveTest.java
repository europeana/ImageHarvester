import akka.event.LoggingAdapter;
import categories.UnitTest;
import com.google.common.io.Files;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaDataUtils;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTuple;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseType;
import gr.ntua.image.mediachecker.MediaChecker;
import org.apache.logging.log4j.LogManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

/**
 * Created by salexandru on 25.05.2015.
 */

@Ignore
@RunWith (MockitoJUnitRunner.class)
@Category (UnitTest.class)
public class HarvesterSlaveTest {
    private static String PATH_PREFIX = Paths.get("src/test/resources/").toAbsolutePath().toString() + "/" ;
    private static String PATH_COLORMAP = PATH_PREFIX + "colormap.png";

    @Mock
    private LoggingAdapter LOG;

    @Mock
    private HttpRetrieveConfig httpRetrieveConfig;

    @Mock
    private RetrieveUrl retrieveUrlTask;

    @Mock
    private ProcessingJobTaskDocumentReference metaInfoTupleTask;

    @Mock
    private ProcessingJobSubTask metaInfoTupleSubTask;

    private SlaveDownloader downloader;

    HttpRetrieveResponseFactory httpRetrieveResponseFactory;

    @Before
    public void setUp() {
        downloader = new SlaveDownloader(LogManager.getLogger(this.getClass().getName()));
        httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();
        when(retrieveUrlTask.getHttpRetrieveConfig()).thenReturn(httpRetrieveConfig);
        when(metaInfoTupleTask.getProcessingTasks()).thenReturn(Arrays.asList(metaInfoTupleSubTask));
        when(metaInfoTupleTask.getTaskType()).thenReturn(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);
    }

    @After
    public void tearDown() {
        final File downloaded = new File(PATH_PREFIX + "downloaded/");

        for (final File file: downloaded.listFiles()) {
            file.delete();
        }
    }

    @Test
    public void test_LinkChecking_ValidUrl() throws Exception {
        final String url = "http://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType.CHECK_LINK);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn("simplyIgnore");

        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, PATH_PREFIX+"1");

        downloader.downloadAndStoreInHttpRetrieveResponse(response,retrieveUrlTask);

        assertEquals( (Integer)HttpURLConnection.HTTP_OK, response.getHttpResponseCode());
        assertEquals(new URL(url), response.getUrl());
    }

    @Test
    public void test_LinkChecking_InvalidUrl() throws Exception {
        final String url = UUID.randomUUID().toString();


        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType.CHECK_LINK);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn("simplyIgnore");

        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, PATH_PREFIX+"1");

        downloader.downloadAndStoreInHttpRetrieveResponse(response,retrieveUrlTask);

        assertEquals((Integer) (-1), response.getHttpResponseCode());
    }


    @Test
    public void test_UnconditionalDownload_ImgUrl() throws Exception {
        final String fileName = "image1.jpeg";
        final String url = "http://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn(fileName);

        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, PATH_PREFIX+"1");

        downloader.downloadAndStoreInHttpRetrieveResponse(response,retrieveUrlTask);

        final byte[] originalFileData = Files.toByteArray(new File(PATH_PREFIX + fileName));

        assertEquals ((Integer) HttpURLConnection.HTTP_OK, response.getHttpResponseCode());
        assertEquals(PATH_PREFIX + "downloaded/" + fileName, response.getAbsolutePath());
        assertEquals(originalFileData.length, response.getContent().length);
        assertArrayEquals(originalFileData, response.getContent());
        assertArrayEquals(originalFileData, Files.toByteArray(new File(response.getAbsolutePath())));
    }

    @Test
    public void test_ConditionalDownload_DownloadImage_ImageDoesNotExist() throws Exception {
        final String fileName = "image2.jpeg";
        final String url = "http://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image2.jpeg";

        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType
                                                                                       .CONDITIONAL_DOWNLOAD);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn(fileName);

        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, PATH_PREFIX+"downloaded");
        downloader.downloadAndStoreInHttpRetrieveResponse(response,retrieveUrlTask);

        assertEquals((Integer) HttpURLConnection.HTTP_OK, response.getHttpResponseCode());
    }

    @Test
    @Ignore
    public void test_ConditionalDownload_DoNotDownloadImage_ImageExists() throws Exception {

    }

    @Test
    @Ignore
    public void test_ConditionalDownload_ReDownloadImage_ImageModified() throws Exception {

    }


    @Test
    public void test_ContentTypeDetection_ImgJpeg() {
        assertEquals(ContentType.IMAGE, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "image1.jpeg"));
        assertEquals (ContentType.IMAGE, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "image2.jpeg"));
    }

    @Test
    public void test_ContentTypeDetection_TextPdf() {
        assertEquals (ContentType.TEXT, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "text1.pdf"));
        assertEquals (ContentType.TEXT, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "text2.pdf"));
    }

    @Test
    public void test_ContentTypeDetection_AudioMp3() {
        assertEquals (ContentType.AUDIO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "audio1.mp3"));
        assertEquals (ContentType.AUDIO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "audio2.mp3"));
    }

    @Test
    public void test_ContentTypeDetection_VideoMpg() {
        assertEquals (ContentType.VIDEO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "video1.mpg"));
        assertEquals (ContentType.VIDEO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "video2.mpg"));
    }

    @Test
    public void test_ContentTypeDetection_Unknown() {
        assertEquals(ContentType.UNKNOWN, MediaMetaDataUtils.classifyUrl("http://www.google.com"));
    }

    @Test
    public void test_MetadataExtraction_Img1() throws Exception {
        final String fileName = "image1.jpeg";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final ImageMetaInfo metaInfo = metaInfoTuple.getImageMetaInfo();

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals("image/jpeg", metaInfo.getMimeType());
        assertEquals((Long) 1399538L, metaInfo.getFileSize());
        assertEquals((Integer) 2500, metaInfo.getWidth());
        assertEquals((Integer) 1737, metaInfo.getHeight());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
        assertTrue("sRGB".equalsIgnoreCase(metaInfo.getColorSpace()));
        assertTrue("JPEG".equalsIgnoreCase(metaInfo.getFileFormat()));
    }

    @Test
    public void test_MetadataExtraction_Img2() throws Exception {
        final String fileName = "image2.jpeg";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final ImageMetaInfo metaInfo = metaInfoTuple.getImageMetaInfo();

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals("image/jpeg", metaInfo.getMimeType());
        assertEquals((Long) 1249616L, metaInfo.getFileSize());
        assertEquals((Integer) 2500, metaInfo.getWidth());
        assertEquals((Integer) 1702, metaInfo.getHeight());
        assertTrue("sRGB".equalsIgnoreCase(metaInfo.getColorSpace()));
        assertTrue("JPEG".equalsIgnoreCase(metaInfo.getFileFormat()));
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
    }

    @Test
    public void test_MetaDataExtraction_Audio1() throws Exception {
        final String fileName = "audio1.mp3";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final AudioMetaInfo metaInfo = metaInfoTuple.getAudioMetaInfo();

        assertNotNull("Audio meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals("audio/mpeg", metaInfo.getMimeType());
        assertEquals((Long) 1388197L, metaInfo.getFileSize());
        assertEquals((Long) 198313L, metaInfo.getDuration());
        assertEquals((Integer)56000, metaInfo.getBitRate());
        assertEquals((Integer)22050, metaInfo.getSampleRate());
        assertEquals((Integer) 1, metaInfo.getChannels());
        assertEquals((Integer) 16, metaInfo.getBitDepth());
        assertTrue("MP3".equalsIgnoreCase(metaInfo.getFileFormat()));
    }

    @Test
    public void test_MetaDataExtraction_Audio2() throws Exception {
        final String fileName = "audio2.mp3";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final AudioMetaInfo metaInfo = metaInfoTuple.getAudioMetaInfo();

        assertNotNull("Audio meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals("audio/mpeg", metaInfo.getMimeType());
        assertEquals((Long) 1234779L, metaInfo.getFileSize());
        assertEquals((Long) 176397L,  metaInfo.getDuration());
        assertEquals((Integer)56000, metaInfo.getBitRate());
        assertEquals((Integer)22050, metaInfo.getSampleRate());
        assertEquals((Integer) 1, metaInfo.getChannels());
        assertEquals((Integer) 16, metaInfo.getBitDepth());
        assertTrue("MP3".equalsIgnoreCase(metaInfo.getFileFormat()));
    }

    @Test
    public void test_MetaDataExtraction_Video1() throws Exception {
        final String fileName = "video1.mpg";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final VideoMetaInfo metaInfo = metaInfoTuple.getVideoMetaInfo();

        assertNotNull("Video meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals((Long) 7662202L, metaInfo.getFileSize());
        assertEquals((Integer) 1150000, metaInfo.getBitRate());
        assertEquals((Integer) 288, metaInfo.getHeight());
        assertEquals((Integer) 352, metaInfo.getWidth());
        assertEquals("video/mpeg", metaInfo.getMimeType());
        assertEquals ("352x288", metaInfo.getResolution());
        assertEquals((Double) 25.0, metaInfo.getFrameRate());
        assertEquals("mpeg1video", metaInfo.getCodec());
        assertEquals((Long) 44745L, metaInfo.getDuration());
    }

    @Test
    public void test_MetaDataExtraction_Video2() throws Exception {
        final String fileName = "video2.mpg";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final VideoMetaInfo metaInfo = metaInfoTuple.getVideoMetaInfo();

        assertNotNull("Video meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals((Long) 9151124L, metaInfo.getFileSize());
        assertEquals((Integer) 1152000, metaInfo.getBitRate());
        assertEquals((Integer) 288, metaInfo.getHeight());
        assertEquals((Integer) 352, metaInfo.getWidth());
        assertEquals("video/mpeg", metaInfo.getMimeType());
        assertEquals ("352x288", metaInfo.getResolution());
        assertEquals ((Double)25.0, metaInfo.getFrameRate());
        assertEquals("mpeg1video", metaInfo.getCodec());
        assertEquals((Long) 53628L, metaInfo.getDuration());
    }

    @Test
    public void test_MetaDataExtraction_Text1() throws Exception {
        final String fileName = "text1.pdf";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX +
                                                                                                           fileName);
        final TextMetaInfo metaInfo = metaInfoTuple.getTextMetaInfo();

        assertNotNull("Text meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNotNull(metaInfo);
        assertEquals((Long)7904453L, metaInfo.getFileSize());
        assertTrue(metaInfo.getIsSearchable());
        assertEquals((Integer) (-1), metaInfo.getResolution());
    }

    @Test
    public void test_MetaDataExtraction_Text2() throws Exception {
        final String fileName = "text2.pdf";

        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(PATH_PREFIX + fileName);
        final TextMetaInfo metaInfo = metaInfoTuple.getTextMetaInfo();

        assertNotNull("Text meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertEquals((Long)13566851L, metaInfo.getFileSize());
        assertFalse(metaInfo.getIsSearchable());
        assertEquals ( (Integer)(-1), metaInfo.getResolution());
    }

    @Test
    public void test_ColorPaletteExtraction_Image1() throws Exception {
        final String fileName = "image1.jpeg";


        final ImageMetaInfo metaInfo = new ColorExtractor(PATH_COLORMAP).colorExtraction(PATH_PREFIX + fileName);

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull(metaInfo.getMimeType());
        assertNull(metaInfo.getFileSize());
        assertNull(metaInfo.getWidth());
        assertNull(metaInfo.getHeight());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
        assertNull(metaInfo.getColorSpace());
        assertNull(metaInfo.getFileFormat());
    }

    @Test
    public void test_ColorPaletteExtraction_Image2() throws Exception {
        final String fileName = "image2.jpeg";


        final ImageMetaInfo metaInfo = new ColorExtractor(PATH_COLORMAP).colorExtraction(PATH_PREFIX + fileName);

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull(metaInfo.getMimeType());
        assertNull(metaInfo.getFileSize());
        assertNull(metaInfo.getWidth());
        assertNull(metaInfo.getHeight());
        assertArrayEquals(MediaChecker.getImageInfo(PATH_PREFIX + fileName, PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
        assertNull(metaInfo.getColorSpace());
        assertNull(metaInfo.getFileFormat());
    }

    @Test
    public void test_ThumbnailGeneration_Image1_Small() throws Exception {
        final String fileName = "image1.jpeg";

        final Integer width = ThumbnailType.SMALL.getWidth();
        final Integer height = ThumbnailType.SMALL.getHeight();

        final File file = new File(PATH_PREFIX + fileName);
        final MediaFile thumbnail = new ThumbnailGenerator(PATH_COLORMAP).createMediaFileWithThumbnail(height, width,
                                                                                                       "",
                                                                                                       PATH_PREFIX +
                                                                                                               fileName,
                                                                                                       Files.toByteArray(file),
                                                                                                       PATH_PREFIX +
                                                                                                               fileName);

        assertEquals("image/jpeg", thumbnail.getContentType());
        assertEquals("", thumbnail.getSource());
        assertEquals(width, thumbnail.getSize());
        assertEquals(PATH_PREFIX + fileName, thumbnail.getOriginalUrl());
        assertEquals(fileName, thumbnail.getName());

      //  Files.write(thumbnail.getContent(), new File(PATH_PREFIX + "image1_thumbnail_small.jpeg"));
    }

    @Test
    public void test_ThumbnailGeneration_Image1_Medium() throws Exception {
        final String fileName = "image1.jpeg";

        final Integer width = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();

        final File file = new File(PATH_PREFIX + fileName);
        final MediaFile thumbnail = new ThumbnailGenerator(PATH_COLORMAP).createMediaFileWithThumbnail(height, width,
                                                                                                       "",
                                                                                                       PATH_PREFIX +
                                                                                                               fileName,
                                                                                                       Files.toByteArray(file),
                                                                                                       PATH_PREFIX +
                                                                                                               fileName);

        assertEquals("image/jpeg", thumbnail.getContentType());
        assertEquals("", thumbnail.getSource());
        assertEquals(width, thumbnail.getSize());
        assertEquals(PATH_PREFIX + fileName, thumbnail.getOriginalUrl());
        assertEquals(fileName, thumbnail.getName());

      //  Files.write(thumbnail.getContent(), new File(PATH_PREFIX + "image1_thumbnail_medium.jpeg"));
    }

    @Test
    public void test_ThumbnailGeneration_Image1_Large() throws Exception {
        final String fileName = "image1.jpeg";

        final Integer width = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();

        final File file = new File(PATH_PREFIX + fileName);
        final MediaFile thumbnail = new ThumbnailGenerator(PATH_COLORMAP).createMediaFileWithThumbnail(height, width,
                                                                                                       "",
                                                                                                       PATH_PREFIX +
                                                                                                               fileName,
                                                                                                       Files.toByteArray(file),
                                                                                                       PATH_PREFIX +
                                                                                                               fileName);

        assertEquals("image/jpeg", thumbnail.getContentType());
        assertEquals("", thumbnail.getSource());
        assertEquals(width, thumbnail.getSize());
        assertEquals(PATH_PREFIX + fileName, thumbnail.getOriginalUrl());
        assertEquals(fileName, thumbnail.getName());

        //Files.write(thumbnail.getContent(), new File(PATH_PREFIX + "image1_thumbnail_large.jpeg"));

    }

    @Test
    public void test_ThumbnailGeneration_Image2_Small() throws Exception {
        final String fileName = "image2.jpeg";

        final Integer width = ThumbnailType.SMALL.getWidth();
        final Integer height = ThumbnailType.SMALL.getHeight();

        final File file = new File(PATH_PREFIX + fileName);
        final MediaFile thumbnail = new ThumbnailGenerator(PATH_COLORMAP).createMediaFileWithThumbnail(height, width,
                                                                                                       "",
                                                                                                       PATH_PREFIX + fileName,
                                                                                                       Files.toByteArray(file),
                                                                                                       PATH_PREFIX + fileName);

        assertEquals("image/jpeg", thumbnail.getContentType());
        assertEquals("", thumbnail.getSource());
        assertEquals(width, thumbnail.getSize());
        assertEquals(PATH_PREFIX + fileName, thumbnail.getOriginalUrl());
        assertEquals(fileName, thumbnail.getName());

       // Files.write(thumbnail.getContent(), new File(PATH_PREFIX + "image2_thumbnail_small.jpeg"));
    }

    @Test
    public void test_ThumbnailGeneration_Image2_Medium() throws Exception {
        final String fileName = "image2.jpeg";

        final Integer width = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();

        final File file = new File(PATH_PREFIX + fileName);
        final MediaFile thumbnail = new ThumbnailGenerator(PATH_COLORMAP).createMediaFileWithThumbnail(height, width,
                                                                                                       "",
                                                                                                       PATH_PREFIX +
                                                                                                               fileName,
                                                                                                       Files.toByteArray(file),
                                                                                                       PATH_PREFIX +
                                                                                                               fileName);

        assertEquals("image/jpeg", thumbnail.getContentType());
        assertEquals("", thumbnail.getSource());
        assertEquals(width, thumbnail.getSize());
        assertEquals(PATH_PREFIX + fileName, thumbnail.getOriginalUrl());
        assertEquals(fileName, thumbnail.getName());

       // Files.write(thumbnail.getContent(), new File(PATH_PREFIX + "image2_thumbnail_medium.jpeg"));
    }

    @Test
    public void test_ThumbnailGeneration_Image2_Large() throws Exception {
        final String fileName = "image2.jpeg";

        final Integer width = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();

        final File file = new File(PATH_PREFIX + fileName);
        final MediaFile thumbnail = new ThumbnailGenerator(PATH_COLORMAP).createMediaFileWithThumbnail(height, width,
                                                                                                       "",
                                                                                                       PATH_PREFIX +
                                                                                                               fileName,
                                                                                                       Files.toByteArray(file),
                                                                                                       PATH_PREFIX +
                                                                                                               fileName);

        assertEquals("image/jpeg", thumbnail.getContentType());
        assertEquals("", thumbnail.getSource());
        assertEquals(width, thumbnail.getSize());
        assertEquals(PATH_PREFIX + fileName, thumbnail.getOriginalUrl());
        assertEquals(fileName, thumbnail.getName());

       // Files.write(thumbnail.getContent(), new File(PATH_PREFIX + "image2_thumbnail_large.jpeg"));

    }

    @Test
    @Ignore
    public void test_DeleteDownloadedFile_ProcessingSuccess() throws Exception {

    }

}
