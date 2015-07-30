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
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RetrieveAndProcessActorTests {
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


}
