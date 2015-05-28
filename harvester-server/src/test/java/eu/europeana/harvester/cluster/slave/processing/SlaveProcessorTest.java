package eu.europeana.harvester.cluster.slave.processing;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import eu.europeana.harvester.cluster.domain.messages.RetrieveUrl;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloader;
import eu.europeana.harvester.cluster.slave.downloading.SlaveDownloaderTest;
import eu.europeana.harvester.cluster.slave.downloading.SlaveLinkChecker;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGenerator;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.filesystem.FileSystemMediaStorageClientImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.HttpRetrieveConfig;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponse;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.harvester.httpclient.response.ResponseType;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class SlaveProcessorTest {

    private static final String PATH_COLORMAP = Paths.get("harvester-server/src/test/resources/").toAbsolutePath().toString() + "/" + "colormap.png";

    private static final String pathOnDisk = Paths.get("harvester-server/src/test/resources/downloader").toAbsolutePath().toString() + "/" + "current_image1.jpeg";
    private static final String text1GitHubUrl = "https://raw.githubusercontent.com/europeana/ImageHarvester/master/harvester-server/src/test/resources/image1.jpeg";

    private static final String FILESYSTEM_PATH_PREFIX = Paths.get("harvester-server/src/test/resources/filesystem").toAbsolutePath().toString() + "/";

    private static final MediaStorageClient client = new FileSystemMediaStorageClientImpl(FILESYSTEM_PATH_PREFIX);
    final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

    private static SlaveProcessor slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(PATH_COLORMAP), new ThumbnailGenerator(PATH_COLORMAP), new ColorExtractor(PATH_COLORMAP), client,
            null);

    private static MetricRegistry metrics = new MetricRegistry();

    @Before
    public void setup() throws IOException {
        FileUtils.forceMkdir(new File(FILESYSTEM_PATH_PREFIX));
    }

    @After
    public void teardown() throws IOException {
        FileUtils.deleteDirectory(new File(FILESYSTEM_PATH_PREFIX));
    }


    @Test
    public void canHandleTypicalProcessingTasks() throws Exception {
        final SlaveDownloader slaveDownloader = new SlaveDownloader(LogManager.getLogger(SlaveDownloaderTest.class.getName()));

        final HttpRetrieveResponse response = httpRetrieveResponseFactory.create(ResponseType.DISK_STORAGE, pathOnDisk);

        final HttpRetrieveConfig httpRetrieveConfig = new HttpRetrieveConfig(
                Duration.millis(0),
                0l,
                0l,
                5 * 1000l, /* terminationThresholdReadPerSecondInBytes */
                Duration.standardSeconds(10) /* terminationThresholdTimeLimit */,
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, /* taskType */
                (int) Duration.standardSeconds(10).getMillis() /* connectionTimeoutInMillis */,
                10 /* maxNrOfRedirects */
        );

        final ProcessingJobSubTask colorExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION, null);
        final ProcessingJobSubTask metaInfoExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null);
        final ProcessingJobSubTask smallThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(180, 180)));
        final ProcessingJobSubTask mediumThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(200, 200)));
        final ProcessingJobSubTask largeThumbnailExtractionSubTask = new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, new GenericSubTaskConfiguration(new ThumbnailConfig(400, 400)));

        final List<ProcessingJobSubTask> subTasks = Lists.newArrayList(
                colorExtractionSubTask,
                metaInfoExtractionSubTask,
                smallThumbnailExtractionSubTask,
                mediumThumbnailExtractionSubTask,
                largeThumbnailExtractionSubTask
        );

        final RetrieveUrl task = new RetrieveUrl("id-1", text1GitHubUrl, httpRetrieveConfig, "jobid-1",
                "referenceid-1", Collections.<String, String>emptyMap(),
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        "source-reference-1", subTasks), null);

        slaveDownloader.downloadAndStoreInHttpRetrieveResponse(response, task);

        final ProcessingResultTuple result = slaveProcessor.process(task.getDocumentReferenceTask(), pathOnDisk, task.getUrl(), Files.readAllBytes(Paths.get(pathOnDisk)), ResponseType.DISK_STORAGE, null);
    }
}
