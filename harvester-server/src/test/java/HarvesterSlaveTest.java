import akka.event.Logging;
import akka.event.LoggingAdapter;
import com.google.common.io.Files;
import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.ProcessorHelperDownload;
import eu.europeana.harvester.cluster.slave.ProcessorHelperProcess;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;

import eu.europeana.harvester.httpclient.response.ResponseType;
import eu.europeana.harvester.utils.FileUtils;
import eu.europeana.harvester.utils.MediaMetaDataUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.pdfbox.util.ImageIOUtil;
import org.junit.After;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Created by salexandru on 25.05.2015.
 */
@RunWith (MockitoJUnitRunner.class)
public class HarvesterSlaveTest {
    private static String PATH_PREFIX = Paths.get("./src/test/resources/").toAbsolutePath().toString();
    private static String PATH_COLORMAP = Paths.get("./colormap.png").toAbsolutePath().toString();

    private ProcessorHelperDownload downloader;

    @Mock
    private LoggingAdapter LOG;

    @Mock
    private HttpRetrieveConfig httpRetrieveConfig;

    @Mock
    private RetrieveUrl retrieveUrlTask;

    @Mock
    private ProcessingJobTaskDocumentReference processingTask;

    @Mock
    private ProcessingJobSubTask processingSubTask;


    @Before
    public void setUp() {
        downloader = new ProcessorHelperDownload(LOG);
        when(retrieveUrlTask.getHttpRetrieveConfig()).thenReturn(httpRetrieveConfig);
        when(processingTask.getProcessingTasks()).thenReturn(Arrays.asList(processingSubTask));
        when(processingTask.getTaskType()).thenReturn(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);
    }

    @After
    public void tearDown() {
        final File downloaded = new File(PATH_PREFIX + "downloaded/");

        for (final File file: downloaded.listFiles()) {
            file.delete();
        }
    }

    @Test
    public void test_LinkChecking_ValidUrl() throws MalformedURLException {
        final String url = "http://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType.CHECK_LINK);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn("simplyIgnore");

        final HttpRetrieveResponse response = downloader.downloadTask(retrieveUrlTask, "ignore");

        assertEquals( (Integer)HttpURLConnection.HTTP_OK, response.getHttpResponseCode());
        assertEquals(new URL(url), response.getUrl());
    }

    @Test
    public void test_LinkChecking_InvalidUrl() throws MalformedURLException {
        final String url = UUID.randomUUID().toString();


        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType.CHECK_LINK);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn("simplyIgnore");

        final HttpRetrieveResponse response = downloader.downloadTask(retrieveUrlTask, "ignore");

        assertEquals((Integer) (-1), response.getHttpResponseCode());
    }


    @Test
    public void test_UnconditionalDownload_ImgUrl() throws IOException {
        final String fileName = "image1.jpeg";
        final String url = "http://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn(fileName);

        final HttpRetrieveResponse response = downloader.downloadTask(retrieveUrlTask, PATH_PREFIX + "downloaded");
        final byte[] originalFileData = Files.toByteArray(new File(PATH_PREFIX + fileName));

        assertEquals ((Integer) HttpURLConnection.HTTP_OK, response.getHttpResponseCode());
        assertEquals(PATH_PREFIX + "downloaded/" + fileName, response.getAbsolutePath());
        assertEquals(originalFileData.length, response.getContent().length);
        assertArrayEquals(originalFileData, response.getContent());
        assertArrayEquals(originalFileData, Files.toByteArray(new File(response.getAbsolutePath())));
    }

    @Test
    public void test_ConditionalDownload_ImgUrl() throws IOException {
        final String fileName = "image2.jpeg";
        final String url = "http://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image2.jpeg";

        when(retrieveUrlTask.getHttpRetrieveConfig().getTaskType()).thenReturn(DocumentReferenceTaskType
                                                                                       .UNCONDITIONAL_DOWNLOAD);
        when(retrieveUrlTask.getUrl()).thenReturn(url);
        when(retrieveUrlTask.getReferenceId()).thenReturn(fileName);

        final HttpRetrieveResponse response = downloader.downloadTask(retrieveUrlTask, PATH_PREFIX + "downloaded");

        assertEquals((Integer) HttpURLConnection.HTTP_OK, response.getHttpResponseCode());
    }

    @Test
    @Ignore
    public void test_ContentTypeDetection_ImgJpeg() {
        assertEquals(ContentType.IMAGE, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "image1.jpeg"));
        assertEquals (ContentType.IMAGE, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "image2.jpeg"));
    }

    @Test
    @Ignore
    public void test_ContentTypeDetection_TextPdf() {
        assertEquals (ContentType.TEXT, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "text1.pdf"));
        assertEquals (ContentType.TEXT, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "text2.pdf"));
    }

    @Test
    @Ignore
    public void test_ContentTypeDetection_AudioMp3() {
        assertEquals (ContentType.AUDIO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "audio1.mp3"));
        assertEquals (ContentType.AUDIO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "audio2.mp3"));
    }

    @Test
    @Ignore
    public void test_ContentTypeDetection_VideoMpg() {
        assertEquals (ContentType.VIDEO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "video1.mpg"));
        assertEquals (ContentType.VIDEO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "video2.mpg"));
    }

    @Test
    @Ignore
    public void test_ContentTypeDetection_Unknown() {
        assertEquals(ContentType.UNKNOWN, MediaMetaDataUtils.classifyUrl("http://www.google.com"));
    }

    @Test
    public void test_MetadataExtraction_Img1() throws Exception {
        final String fileName = "image1.jpeg";
        final HttpRetrieveResponse response = mock(HttpRetrieveResponse.class);

        when(response.getAbsolutePath()).thenReturn("");

        final DoneDownload doneDownload = new DoneDownload("", "", "", "",
                                                           ProcessingState.SUCCESS,
                                                           response,
                                                           processingTask,
                                                           ""
                                                           );

        when(processingSubTask.getTaskType()).thenReturn(ProcessingJobSubTaskType.META_EXTRACTION);

        final ImageMetaInfo metaInfo = processor.startProcessing(processingTask,
                                                                 doneDownload,
                                                                 PATH_PREFIX + fileName,
                                                                 PATH_COLORMAP,
                                                                 null,
                                                                 ResponseType.DISK_STORAGE,
                                                                 null
                                                                ).getImageMetaInfo();

        assertNotNull ("Image meta info must not be null!", metaInfo);
        assertEquals("image/jpeg", metaInfo.getMimeType());
        assertEquals((Long) 1399538L, metaInfo.getFileSize());
        assertEquals((Integer) 2500, metaInfo.getWidth());
        assertEquals((Integer) 1737, metaInfo.getHeight());
        assertTrue("RGB".equalsIgnoreCase(metaInfo.getColorSpace()));
        assertEquals  ("image/jpeg", metaInfo.getFileFormat());
    }

    @Test
    @Ignore
    public void test_MetadataExtraction_Img2() throws Exception {
        final String fileName = "image2.jpeg";
        final HttpRetrieveResponse response = mock(HttpRetrieveResponse.class);

        when(response.getAbsolutePath()).thenReturn("");
        final DoneDownload doneDownload = new DoneDownload("", "", "", "",
                                                           ProcessingState.SUCCESS,
                                                           response,
                                                           processingTask,
                                                           ""
        );

        when(processingSubTask.getTaskType()).thenReturn(ProcessingJobSubTaskType.META_EXTRACTION);

        final ImageMetaInfo metaInfo = processor.startProcessing(processingTask,
                                                                 doneDownload,
                                                                 PATH_PREFIX + fileName,
                                                                 PATH_COLORMAP,
                                                                 null,
                                                                 ResponseType.DISK_STORAGE,
                                                                 null
                                                                ).getImageMetaInfo();

        assertNotNull (metaInfo);
        assertEquals("image/jpeg", metaInfo.getMimeType());
        assertEquals((Long) 1249616L, metaInfo.getFileSize());
        assertEquals((Integer) 2500, metaInfo.getWidth());
        assertEquals((Integer) 1702, metaInfo.getHeight());
        assertTrue("RGB".equalsIgnoreCase(metaInfo.getColorSpace()));
        assertEquals  ("image/jpeg", metaInfo.getFileFormat());
    }

    @Test
    @Ignore
    public void test_MetaDataExtraction_Audio1() throws Exception {
        final String fileName = "audio1.mp3";
        final HttpRetrieveResponse response = mock(HttpRetrieveResponse.class);

        when(response.getAbsolutePath()).thenReturn("");
        final DoneDownload doneDownload = new DoneDownload("", "", "", "",
                                                           ProcessingState.SUCCESS,
                                                           response,
                                                           processingTask,
                                                           ""
        );

        when(processingSubTask.getTaskType()).thenReturn(ProcessingJobSubTaskType.META_EXTRACTION);

        final AudioMetaInfo metaInfo = processor.startProcessing(processingTask,
                                                                 doneDownload,
                                                                 PATH_PREFIX + fileName,
                                                                 PATH_COLORMAP,
                                                                 null,
                                                                 ResponseType.DISK_STORAGE,
                                                                 null
                                                                ).getAudioMetaInfo();
    }

    @Test
    @Ignore
    public void test_MetaDataExtraction_Audio2() throws Exception {
        final String fileName = "audio2.mp3";
        final HttpRetrieveResponse response = mock(HttpRetrieveResponse.class);

        when(response.getAbsolutePath()).thenReturn("");
        final DoneDownload doneDownload = new DoneDownload("", "", "", "",
                                                           ProcessingState.SUCCESS,
                                                           response,
                                                           processingTask,
                                                           ""
        );

        when(processingSubTask.getTaskType()).thenReturn(ProcessingJobSubTaskType.META_EXTRACTION);

        final AudioMetaInfo metaInfo = processor.startProcessing(processingTask,
                                                                 doneDownload,
                                                                 PATH_PREFIX + fileName,
                                                                 PATH_COLORMAP,
                                                                 null,
                                                                 ResponseType.DISK_STORAGE,
                                                                 null
                                                                ).getAudioMetaInfo();
    }

    @Test
    @Ignore
    public void test_MetaDataExtraction_Video1() throws Exception {
        final String fileName = "video1.mpg";
        final HttpRetrieveResponse response = mock(HttpRetrieveResponse.class);

        when(response.getAbsolutePath()).thenReturn("");
        final DoneDownload doneDownload = new DoneDownload("", "", "", "",
                                                           ProcessingState.SUCCESS,
                                                           response,
                                                           processingTask,
                                                           ""
        );

        when(processingSubTask.getTaskType()).thenReturn(ProcessingJobSubTaskType.META_EXTRACTION);

        final VideoMetaInfo metaInfo = processor.startProcessing(processingTask,
                                                                 doneDownload,
                                                                 PATH_PREFIX + fileName,
                                                                 PATH_COLORMAP,
                                                                 null,
                                                                 ResponseType.DISK_STORAGE,
                                                                 null
                                                                ).getVideoMetaInfo();
    }

    @Test
    @Ignore
    public void test_MetaDataExtraction_Video2() throws Exception {
        final String fileName = "video2.mpg";
        final HttpRetrieveResponse response = mock(HttpRetrieveResponse.class);

        when(response.getAbsolutePath()).thenReturn("");
        final DoneDownload doneDownload = new DoneDownload("", "", "", "",
                                                           ProcessingState.SUCCESS,
                                                           response,
                                                           processingTask,
                                                           ""
        );

        when(processingSubTask.getTaskType()).thenReturn(ProcessingJobSubTaskType.META_EXTRACTION);

        final VideoMetaInfo metaInfo = processor.startProcessing(processingTask,
                                                                 doneDownload,
                                                                 PATH_PREFIX + fileName,
                                                                 PATH_COLORMAP,
                                                                 null,
                                                                 ResponseType.DISK_STORAGE,
                                                                 null
                                                                ).getVideoMetaInfo();
    }
}
