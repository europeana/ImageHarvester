package eu.europeana.harvester.cluster.slave;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import eu.europeana.harvester.cluster.domain.messages.DoneDownload;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrlWithProcessingConfig;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessor;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.filesystem.FileSystemMediaStorageClientImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.RetrievingState;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class RetrieveAndProcessActorTest {
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    private static String PROCESSING_PATH_PREFIX = Paths.get("harvester-server/src/test/resources/processing").toAbsolutePath().toString() + "/";
    private static final String text1GitHubUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

    private static final String PATH_PREFIX = Paths.get("src/test/resources/").toAbsolutePath().toString() + "/" ;
    private static final String PATH_COLORMAP = PATH_PREFIX + "colormap.png";

    private static final String FILESYSTEM_PATH_PREFIX = Paths.get("src/test/resources/filesystem").toAbsolutePath().toString() + "/";

    private static final MediaStorageClient client = new FileSystemMediaStorageClientImpl(FILESYSTEM_PATH_PREFIX);
    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    static ActorSystem system ;

    @BeforeClass
    public static void setup() throws IOException {
        FileUtils.forceMkdir(new File(FILESYSTEM_PATH_PREFIX));
        FileUtils.forceMkdir(new File(PROCESSING_PATH_PREFIX));
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() throws IOException {
        if (null != system) {
            system.shutdown();
        }
        FileUtils.deleteDirectory(new File(FILESYSTEM_PATH_PREFIX));
        FileUtils.deleteDirectory(new File(PROCESSING_PATH_PREFIX));
    }

    @Test
    public void canRetreievAndProcessTypicalJob() throws Exception {

        final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION,null);
        final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION,null);
        final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(200,200)));
        final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(400,400)));

        final List<ProcessingJobSubTask> subTasks = Lists.newArrayList(
                colorExtractionSubTask,
                metaInfoExtractionSubTask,
                mediumThumbnailExtractionSubTask,
                largeThumbnailExtractionSubTask
        );

        final RetrieveUrl task = new RetrieveUrl(text1GitHubUrl, new ProcessingJobLimits(), DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,"a",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", subTasks), null,new ReferenceOwner("unknown","unknwon","unknown"));

        final RetrieveUrlWithProcessingConfig taskWithConfig = new RetrieveUrlWithProcessingConfig(task,PROCESSING_PATH_PREFIX+task.getId());
    /*
     * Wrap the whole test procedure within a testkit
     * initializer if you want to receive actor replies
     * or use Within(), etc.
     */
        new JavaTestKit(system) {{

            final ActorRef subject = RetrieveAndProcessActor.createActor(getSystem(),httpRetrieveResponseFactory,client,PATH_COLORMAP);

            subject.tell(taskWithConfig, getRef());

            while (!msgAvailable()) Thread.sleep(100);
            DoneDownload msg1 = expectMsgAnyClassOf(DoneDownload.class);
            assertEquals(msg1.getDocumentReferenceTask().getTaskType(),DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);
            assertEquals(msg1.getRetrieveState(), RetrievingState.COMPLETED);

            while (!msgAvailable()) Thread.sleep(100);
            DoneProcessing msg2 = expectMsgAnyClassOf(DoneProcessing.class);

            assertEquals(msg2.getImageMetaInfo().getWidth().intValue(),2500);
            assertEquals(msg2.getImageMetaInfo().getHeight().intValue(),1737);
            assertEquals(msg2.getImageMetaInfo().getMimeType(),"image/jpeg");

            final MediaFile originalStoredContent = client.retrieve(MediaFile.generateIdFromUrlAndSizeType(msg2.getUrl(),"ORIGINAL"),true);
            final MediaFile mediumStoredContent = client.retrieve(MediaFile.generateIdFromUrlAndSizeType(msg2.getUrl(),"MEDIUM"),true);
            final MediaFile largeStoredContent = client.retrieve(MediaFile.generateIdFromUrlAndSizeType(msg2.getUrl(),"LARGE"),true);

            assertEquals("The original stored content is not equal with the original content", new Long(originalStoredContent.getContent().length), msg1.getHttpRetrieveResponse().getContentSizeInBytes());
            assertNotNull(mediumStoredContent);
            assertNotNull(largeStoredContent);

        }};
    }

    @Test
    public void test_CheckLink_Success() throws InterruptedException {
        final RetrieveUrl task = new RetrieveUrl(text1GitHubUrl, new ProcessingJobLimits(), DocumentReferenceTaskType.CHECK_LINK,"a",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                        "source-reference-1", Collections.EMPTY_LIST), null,new ReferenceOwner("unknown","unknwon","unknown"));

        final RetrieveUrlWithProcessingConfig taskWithConfig = new RetrieveUrlWithProcessingConfig(task,PROCESSING_PATH_PREFIX+task.getId());
    /*
     * Wrap the whole test procedure within a testkit
     * initializer if you want to receive actor replies
     * or use Within(), etc.
     */
        new JavaTestKit(system) {{

            final ActorRef subject = RetrieveAndProcessActor.createActor(getSystem(),httpRetrieveResponseFactory,client,PATH_COLORMAP);

            subject.tell(taskWithConfig, getRef());

            while (!msgAvailable()) Thread.sleep(100);
            DoneDownload msg1 = expectMsgAnyClassOf(DoneDownload.class);
            assertEquals(msg1.getDocumentReferenceTask().getTaskType(),DocumentReferenceTaskType.CHECK_LINK);
            assertEquals(msg1.getRetrieveState(), RetrievingState.COMPLETED);

            while (!msgAvailable()) Thread.sleep(100);
            DoneProcessing msg2 = expectMsgAnyClassOf(DoneProcessing.class);

            assertEquals (200, msg2.getHttpResponseCode().intValue());
            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getRetrieveState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getColorExtractionState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getMetaExtractionState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getThumbnailGenerationState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getThumbnailStorageState());
        }};
    }

    @Test
    public void test_CheckLink_Fail() throws InterruptedException {
        final RetrieveUrl task = new RetrieveUrl("http://ana.are.mere:0099909/pere", new ProcessingJobLimits(), DocumentReferenceTaskType.CHECK_LINK,"a",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                        "source-reference-1", Collections.EMPTY_LIST), null,new ReferenceOwner("unknown","unknwon","unknown"));

        final RetrieveUrlWithProcessingConfig taskWithConfig = new RetrieveUrlWithProcessingConfig(task,PROCESSING_PATH_PREFIX+task.getId());
    /*
     * Wrap the whole test procedure within a testkit
     * initializer if you want to receive actor replies
     * or use Within(), etc.
     */
        new JavaTestKit(system) {{

            final ActorRef subject = RetrieveAndProcessActor.createActor(getSystem(),httpRetrieveResponseFactory,client,PATH_COLORMAP);

            subject.tell(taskWithConfig, getRef());

            while (!msgAvailable()) Thread.sleep(100);
            DoneDownload msg1 = expectMsgAnyClassOf(DoneDownload.class);
            assertEquals(msg1.getDocumentReferenceTask().getTaskType(),DocumentReferenceTaskType.CHECK_LINK);
            assertNotEquals(msg1.getRetrieveState(), RetrievingState.COMPLETED);

            while (!msgAvailable()) Thread.sleep(100);
            DoneProcessing msg2 = expectMsgAnyClassOf(DoneProcessing.class);

            assertEquals (ProcessingJobSubTaskState.ERROR, msg2.getProcessingStats().getRetrieveState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getColorExtractionState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getMetaExtractionState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getThumbnailGenerationState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getThumbnailStorageState());
        }};
    }

    @Test
    public void test_ConditionalDownload_Success() throws InterruptedException, NoSuchAlgorithmException, IOException {
        final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION,null);
        final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION,null);
        final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(200,200)));
        final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(400,400)));

        final List<ProcessingJobSubTask> subTasks = Lists.newArrayList(
                colorExtractionSubTask,
                metaInfoExtractionSubTask,
                mediumThumbnailExtractionSubTask,
                largeThumbnailExtractionSubTask
        );

        final RetrieveUrl task = new RetrieveUrl(text1GitHubUrl, new ProcessingJobLimits(), DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,"a",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", subTasks), null,new ReferenceOwner("unknown","unknwon","unknown"));

        final RetrieveUrlWithProcessingConfig taskWithConfig = new RetrieveUrlWithProcessingConfig(task,PROCESSING_PATH_PREFIX+task.getId());
    /*
     * Wrap the whole test procedure within a testkit
     * initializer if you want to receive actor replies
     * or use Within(), etc.
     */
        new JavaTestKit(system) {{

            final ActorRef subject = RetrieveAndProcessActor.createActor(getSystem(),httpRetrieveResponseFactory,client,PATH_COLORMAP);

            subject.tell(taskWithConfig, getRef());

            while (!msgAvailable()) Thread.sleep(100);
            DoneDownload msg1 = expectMsgAnyClassOf(DoneDownload.class);
            assertEquals(msg1.getDocumentReferenceTask().getTaskType(),DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD);
            assertEquals(msg1.getRetrieveState(), RetrievingState.COMPLETED);

            while (!msgAvailable()) Thread.sleep(100);
            DoneProcessing msg2 = expectMsgAnyClassOf(DoneProcessing.class);

            assertEquals(msg2.getImageMetaInfo().getWidth().intValue(),2500);
            assertEquals(msg2.getImageMetaInfo().getHeight().intValue(),1737);
            assertEquals(msg2.getImageMetaInfo().getMimeType(),"image/jpeg");

            final MediaFile originalStoredContent = client.retrieve(MediaFile.generateIdFromUrlAndSizeType(msg2.getUrl(),"ORIGINAL"),true);
            final MediaFile mediumStoredContent = client.retrieve(MediaFile.generateIdFromUrlAndSizeType(msg2.getUrl(),"MEDIUM"),true);
            final MediaFile largeStoredContent = client.retrieve(MediaFile.generateIdFromUrlAndSizeType(msg2.getUrl(),"LARGE"),true);

            assertEquals (200, msg2.getHttpResponseCode().intValue());
            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getRetrieveState());
            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getColorExtractionState());
            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getMetaExtractionState());
            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getThumbnailGenerationState());
            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getThumbnailStorageState());
            assertEquals("The original stored content is not equal with the original content", new Long(originalStoredContent.getContent().length), msg1.getHttpRetrieveResponse().getContentSizeInBytes());
            assertNotNull(mediumStoredContent);
            assertNotNull(largeStoredContent);

        }};
    }

    @Test
    public void test_ConditionalDownload_LinkFail() throws InterruptedException, NoSuchAlgorithmException, IOException {
        final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION,null);
        final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION,null);
        final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(200,200)));
        final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(400,400)));

        final List<ProcessingJobSubTask> subTasks = Lists.newArrayList(
                colorExtractionSubTask,
                metaInfoExtractionSubTask,
                mediumThumbnailExtractionSubTask,
                largeThumbnailExtractionSubTask
        );

        final RetrieveUrl task = new RetrieveUrl("http://random-node/ana/are/mere/multe/si/proaste/pi/3?id=1145", new ProcessingJobLimits(), DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,"a",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", subTasks), null,new ReferenceOwner("unknown","unknwon","unknown"));

        final RetrieveUrlWithProcessingConfig taskWithConfig = new RetrieveUrlWithProcessingConfig(task,PROCESSING_PATH_PREFIX+task.getId());
    /*
     * Wrap the whole test procedure within a testkit
     * initializer if you want to receive actor replies
     * or use Within(), etc.
     */
        new JavaTestKit(system) {{

            final ActorRef subject = RetrieveAndProcessActor.createActor(getSystem(),httpRetrieveResponseFactory,client,PATH_COLORMAP);

            subject.tell(taskWithConfig, getRef());

            while (!msgAvailable()) Thread.sleep(100);
            DoneDownload msg1 = expectMsgAnyClassOf(DoneDownload.class);

            while (!msgAvailable()) Thread.sleep(100);
            DoneProcessing msg2 = expectMsgAnyClassOf(DoneProcessing.class);


            assertEquals (ProcessingJobSubTaskState.ERROR, msg2.getProcessingStats().getRetrieveState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getColorExtractionState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getMetaExtractionState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getThumbnailGenerationState());
            assertEquals (ProcessingJobSubTaskState.NEVER_EXECUTED, msg2.getProcessingStats().getThumbnailStorageState());

        }};
    }

    @Test
    public void test_ConditionalDownload_TaskFail() throws Exception {
        final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION,null);
        final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION,null);
        final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(200,200)));
        final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL,new GenericSubTaskConfiguration(new ThumbnailConfig(400,400)));

        final List<ProcessingJobSubTask> subTasks = Lists.newArrayList(
                colorExtractionSubTask,
                metaInfoExtractionSubTask,
                mediumThumbnailExtractionSubTask,
                largeThumbnailExtractionSubTask
        );

        final RetrieveUrl task = new RetrieveUrl("http://random-node/ana/are/mere/multe/si/proaste/pi/3?id=1145", new ProcessingJobLimits(), DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,"a",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", subTasks), null,new ReferenceOwner("unknown","unknwon","unknown"));

        final RetrieveUrlWithProcessingConfig taskWithConfig = new RetrieveUrlWithProcessingConfig(task,PROCESSING_PATH_PREFIX+task.getId());
    /*
     * Wrap the whole test procedure within a testkit
     * initializer if you want to receive actor replies
     * or use Within(), etc.
     */
        MediaMetaInfoExtractor mediaMetaInfoExtractorFail = mock(MediaMetaInfoExtractor.class);
        ThumbnailGenerator thumbnailGeneratorFail = mock(ThumbnailGenerator.class);
        ColorExtractor colorExtractorFail = mock(ColorExtractor.class);
        MediaStorageClient mediaStorageClientFail = mock(FileSystemMediaStorageClientImpl.class);

        doReturn(null).when(mediaMetaInfoExtractorFail).extract(anyString());
        doReturn(null).when(colorExtractorFail).colorExtraction(anyString());
        doReturn(null).when(thumbnailGeneratorFail).createMediaFileWithThumbnail(anyInt(), anyInt(), anyString(),
                anyString(),
                any(new byte[]{}.getClass()),
                anyString());

     final SlaveProcessor slaveProcessorFail = new SlaveProcessor(
            mediaMetaInfoExtractorFail,
             thumbnailGeneratorFail,
             colorExtractorFail,
             mediaStorageClientFail
     )  ;
        new JavaTestKit(system) {{

            final ActorRef subject = RetrieveAndProcessActor.createActor(getSystem(),
                                                                         httpRetrieveResponseFactory,
                                                                         slaveProcessorFail);

            subject.tell(taskWithConfig, getRef());

            while (!msgAvailable()) Thread.sleep(100);
            DoneDownload msg1 = expectMsgAnyClassOf(DoneDownload.class);

            while (!msgAvailable()) Thread.sleep(100);
            DoneProcessing msg2 = expectMsgAnyClassOf(DoneProcessing.class);


            assertEquals (ProcessingJobSubTaskState.SUCCESS, msg2.getProcessingStats().getRetrieveState());
            assertEquals (ProcessingJobSubTaskState.ERROR, msg2.getProcessingStats().getColorExtractionState());
            assertEquals (ProcessingJobSubTaskState.ERROR, msg2.getProcessingStats().getMetaExtractionState());
            assertEquals (ProcessingJobSubTaskState.ERROR, msg2.getProcessingStats().getThumbnailGenerationState());
            assertEquals (ProcessingJobSubTaskState.ERROR, msg2.getProcessingStats().getThumbnailStorageState());
        }};
    }


}
