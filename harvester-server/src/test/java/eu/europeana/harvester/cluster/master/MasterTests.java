package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.JavaTestKit;
import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import eu.europeana.harvester.TestUtils;
import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.*;
import eu.europeana.harvester.cluster.domain.utils.Pair;
import eu.europeana.harvester.cluster.master.jobrestarter.JobRestarterConfig;
import eu.europeana.harvester.cluster.slave.RetrieveAndProcessActor;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessor;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractor;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoExtractor;
import eu.europeana.harvester.db.MediaStorageClient;
import eu.europeana.harvester.db.filesystem.FileSystemMediaStorageClientImpl;
import eu.europeana.harvester.db.interfaces.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.httpclient.response.HttpRetrieveResponseFactory;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.commons.io.FileUtils;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static eu.europeana.harvester.TestUtils.*;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Created by paul on 12/08/15.
 */
public class MasterTests {

    private ProcessingJobDao processingJobDao;
    private HistoricalProcessingJobDao historicalProcessingJobDao;
    private MachineResourceReferenceDao machineResourceReferenceDao;
    private SourceDocumentReferenceDao sourceDocumentReferenceDao;
    private LastSourceDocumentProcessingStatisticsDao lastSourceDocumentProcessingStatisticsDao;
    private SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;
    private SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao;

    private SourceDocumentReferenceProcessingProfileDao sourceDocumentProcessingProfileDao;

    private Datastore datastore;
    private HarvesterClient harvesterClient;
    private MediaStorageClient mediaStorageClient;
    private SlaveProcessor slaveProcessor;


    @Before
    public void setUp() throws Exception {
        MongoClient mongo = new MongoClient("78.46.164.244", 27017);
        Morphia morphia = new Morphia();
        String dbName = "test_crf_europeana_harvester_master";

        final String username = "";
        final String password = "";

        datastore = morphia.createDatastore(mongo, dbName);

        processingJobDao = new ProcessingJobDaoImpl(datastore);
        historicalProcessingJobDao = new HistoricalProcessingJobDaoImpl(datastore);
        machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        lastSourceDocumentProcessingStatisticsDao = new LastSourceDocumentProcessingStatisticsDaoImpl(datastore);
        sourceDocumentProcessingStatisticsDao = new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        sourceDocumentReferenceMetaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);
        sourceDocumentProcessingProfileDao = new SourceDocumentReferenceProcessingProfileDaoImpl(datastore);

        harvesterClient = new HarvesterClientImpl(datastore, new HarvesterClientConfig());

        FileUtils.forceMkdir(new File(PATH_DOWNLOADED));
        mediaStorageClient = new FileSystemMediaStorageClientImpl(PATH_DOWNLOADED);
        slaveProcessor = new SlaveProcessor(new MediaMetaInfoExtractor(PATH_COLORMAP), new ColorExtractor(PATH_COLORMAP), mediaStorageClient, PATH_COLORMAP);
    }

    @After
    public void tearDown() {
        datastore.delete(datastore.createQuery(ProcessingJob.class));
        datastore.delete(datastore.createQuery(HistoricalProcessingJob.class));
        datastore.delete(datastore.createQuery(MachineResourceReference.class));
        datastore.delete(datastore.createQuery(SourceDocumentReference.class));
        datastore.delete(datastore.createQuery(LastSourceDocumentProcessingStatistics.class));
        datastore.delete(datastore.createQuery(SourceDocumentProcessingStatistics.class));
        datastore.delete(datastore.createQuery(SourceDocumentReferenceMetaInfo.class));
        datastore.delete(datastore.createQuery(SourceDocumentReferenceProcessingProfile.class));

    }

    private void createSomeJobs(final List<String> urls, final Integer priority) throws ExecutionException, UnknownHostException, MalformedURLException, TimeoutException, InterruptedException {
        final String collectionId = "test_collection_1";
        final String providerId = "test_provider_1";
        final String recordId = "test_record_1";
        final String executionId = "test_execution_id_1";

        final List<ProcessingJobTuple> result = new ArrayList<ProcessingJobTuple>();
        for (final String url : urls) {
            result.addAll(JobCreator.createJobs(collectionId,
                    providerId,
                    recordId,
                    executionId,
                    url,
                    new ArrayList<String>(),
                    url,
                    url,
                    priority));
        }

        harvesterClient.createOrModifyProcessingJobTuples(result);

    }

    private void createSingleConditionalDownloadJob(final String url, final Integer priority) throws ExecutionException, UnknownHostException, MalformedURLException, TimeoutException, InterruptedException {
        final String collectionId = "test_collection_1";
        final String providerId = "test_provider_1";
        final String recordId = "test_record_1";
        final String executionId = "test_execution_id_1";

        final List<ProcessingJobTuple> result = new ArrayList<ProcessingJobTuple>();
        result.addAll(JobCreator.createJobs(collectionId,
                providerId,
                recordId,
                executionId,
                url,
                new ArrayList<String>(),
                null,
                null,
                priority));

        harvesterClient.createOrModifyProcessingJobTuples(result);

    }


    private Pair<ActorSystem, ActorRef> createAndStartMaster() {

        // (Step 1) Create master

        final Config config = ConfigFactory.empty().withValue("akka.actor.provider", ConfigValueFactory.fromAnyRef("akka.cluster.ClusterActorRefProvider"));
        ActorSystem system = ActorSystem.create("TestClusterSystem", config);
        final Integer jobsPerIP = 10;
        final Duration receiveTimeoutInterval = Duration.standardSeconds(50);
        final Integer responseTimeoutFromSlaveInMillis = (int) Duration.standardSeconds(50).getMillis();
        final Long maxTasksInMemory = 50l;
        final Integer cleanupInterval = 100;
        final JobRestarterConfig jobRestarterConfig = new JobRestarterConfig(Duration.standardDays(100));

        final ClusterMasterConfig clusterMasterConfig = new ClusterMasterConfig(jobsPerIP, maxTasksInMemory,
                receiveTimeoutInterval, responseTimeoutFromSlaveInMillis, jobRestarterConfig, WriteConcern.NORMAL);

        final DefaultLimits defaultLimits = new DefaultLimits(1 /*taskBatchSize*/, 100000l /*defaultBandwidthLimitReadInBytesPerSec*/,
                10 /*defaultMaxConcurrentConnectionsLimit*/, 1000 /*minDistanceInMillisBetweenTwoRequest*/,
                10000 /*connectionTimeoutInMillis*/, 5 /*maxNrOfRedirects*/, 80d /*minTasksPerIPPercentage*/, Duration.standardMinutes(10));

        final IPExceptions ipExceptions = new IPExceptions(100, Collections.EMPTY_LIST, Collections.EMPTY_LIST);

        final Duration delayForCountingTheStateOfDocuments = Duration.standardDays(100);

        final ActorRef clusterMaster = system.actorOf(Props.create(ClusterMasterActor.class,
                clusterMasterConfig,
                ipExceptions,
                processingJobDao,
                historicalProcessingJobDao,
                machineResourceReferenceDao,
                sourceDocumentProcessingStatisticsDao,
                lastSourceDocumentProcessingStatisticsDao,
                sourceDocumentReferenceDao,
                sourceDocumentReferenceMetaInfoDao,
                sourceDocumentProcessingProfileDao,
                defaultLimits,
                cleanupInterval,
                delayForCountingTheStateOfDocuments), "testcClusterMaster");

        // (Step 2) Start master
        clusterMaster.tell(new LoadJobs(), ActorRef.noSender());
        clusterMaster.tell(new Monitor(), ActorRef.noSender());
        clusterMaster.tell(new CheckForTaskTimeout(), ActorRef.noSender());

        return new Pair<>(system, clusterMaster);
    }

    private void stopSystem(final ActorSystem system) {
        system.shutdown();
    }


    @Test
    public void canLoadJobsWithNormalPriorityAndSendThemToTheSlaveAndPersistTheResult() throws
            InterruptedException, ExecutionException, MalformedURLException, TimeoutException, UnknownHostException {
        final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

        createSingleConditionalDownloadJob(TestUtils.GitHubUrl_PREFIX + TestUtils.Image1, JobPriority.FASTLANE.getPriority());

        final Pair<ActorSystem, ActorRef> systemAndMasterActor = createAndStartMaster();
        final ActorRef clusterMaster = systemAndMasterActor.getValue();

        new JavaTestKit(systemAndMasterActor.getKey()) {
            {
                final ActorRef downloadAndProcess1 = RetrieveAndProcessActor.createActor(systemAndMasterActor.getKey(), httpRetrieveResponseFactory, mediaStorageClient, PATH_COLORMAP);
                Thread.sleep(500);

                // (Step 1) Request tasks from master
                clusterMaster.tell(new RequestTasks(), getRef());

                while (!msgAvailable()) {
                    Thread.sleep(100);
                }
                BagOfTasks msg1 = expectMsgAnyClassOf(BagOfTasks.class);
                assertEquals(1, msg1.getTasks().size());
                final RetrieveUrl retrieveUrl1 = msg1.getTasks().get(0);

                // (Step 2) Hand over the first task to the slave
                downloadAndProcess1.tell(new RetrieveUrlWithProcessingConfig(retrieveUrl1, PATH_DOWNLOADED + Image1), getRef());
                while (!msgAvailable()) {
                    Thread.sleep(100);
                }

                // (Step 3) Handover the response to the master
                DoneProcessing doneProcessing1 = expectMsgAnyClassOf(DoneProcessing.class);
                clusterMaster.tell(doneProcessing1, getRef());
                Thread.sleep(500);

                // (Step 4) Verify database

                final ProcessingJob processingJob1 = processingJobDao.read(doneProcessing1.getJobId());
                assertEquals(JobState.FINISHED, processingJob1.getState());

                final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics1 = sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(doneProcessing1.getReferenceId(), doneProcessing1.getJobId());
                assertEquals(sourceDocumentProcessingStatistics1.getHttpResponseCode().longValue(), 200);
                assertEquals(sourceDocumentProcessingStatistics1.getHttpResponseHeaders().size(), 23);
                assertTrue(sourceDocumentProcessingStatistics1.getHttpResponseContentSizeInBytes() > 10000);

                final SourceDocumentReference sourceDocumentReference = sourceDocumentReferenceDao.read(doneProcessing1.getReferenceId());
                assertEquals(sourceDocumentProcessingStatistics1.getId(), sourceDocumentReference.getLastStatsId());

                stopSystem(systemAndMasterActor.getKey());

            }
        };
        stopSystem(systemAndMasterActor.getKey());


    }

    @Test
    public void canLoadJobsWithNormalPriorityAndSendThemToTheSlave() throws
            InterruptedException, ExecutionException, MalformedURLException, TimeoutException, UnknownHostException {
        final HttpRetrieveResponseFactory httpRetrieveResponseFactory = new HttpRetrieveResponseFactory();

        createSingleConditionalDownloadJob(TestUtils.GitHubUrl_PREFIX + TestUtils.Image1, JobPriority.FASTLANE.getPriority());

        final Pair<ActorSystem, ActorRef> systemAndMasterActor = createAndStartMaster();
        final ActorRef clusterMaster = systemAndMasterActor.getValue();

        new JavaTestKit(systemAndMasterActor.getKey()) {{
            final ActorRef downloadAndProcess1 = RetrieveAndProcessActor.createActor(systemAndMasterActor.getKey(), httpRetrieveResponseFactory, mediaStorageClient, PATH_COLORMAP);
            Thread.sleep(500);
            // JOB 1
            // (Step 1) Request tasks from master
            clusterMaster.tell(new RequestTasks(), getRef());

            while (!msgAvailable()) {
                Thread.sleep(100);
            }
            BagOfTasks msg1 = expectMsgAnyClassOf(BagOfTasks.class);
            assertEquals(1, msg1.getTasks().size());
            final RetrieveUrl retrieveUrl1 = msg1.getTasks().get(0);

            // (Step 2) Hand over the first task to the slave
            downloadAndProcess1.tell(new RetrieveUrlWithProcessingConfig(retrieveUrl1, PATH_DOWNLOADED + Image1), getRef());
            while (!msgAvailable()) {
                Thread.sleep(100);
            }

            // (Step 3) Handover the response to the master
            DoneProcessing doneProcessing1 = expectMsgAnyClassOf(DoneProcessing.class);
            clusterMaster.tell(doneProcessing1, getRef());
            Thread.sleep(500);

            // (Step 4) Verify database

            final ProcessingJob processingJob1 = processingJobDao.read(doneProcessing1.getJobId());
            assertEquals(JobState.FINISHED,processingJob1.getState());

            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics1 = sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(doneProcessing1.getReferenceId(),doneProcessing1.getJobId());
            assertEquals(sourceDocumentProcessingStatistics1.getHttpResponseCode().longValue(), 200);
            assertEquals(sourceDocumentProcessingStatistics1.getHttpResponseHeaders().size(), 23);
            assertTrue(sourceDocumentProcessingStatistics1.getHttpResponseContentSizeInBytes() > 10000);
            final SourceDocumentReference sourceDocumentReference1 = sourceDocumentReferenceDao.read(doneProcessing1.getReferenceId());
            assertEquals(sourceDocumentReference1.getLastStatsId(), sourceDocumentProcessingStatistics1.getId());

            // JOB 2
            // (Step 5) Create another conditional download for the same url & trigger database job loading
            createSingleConditionalDownloadJob(TestUtils.GitHubUrl_PREFIX + TestUtils.Image1, JobPriority.FASTLANE.getPriority());
            clusterMaster.tell(new LoadJobs(), getRef());
            Thread.sleep(500);

            // (Step 6) Ask for tasks from the master
            clusterMaster.tell(new RequestTasks(), getRef());

            while (!msgAvailable()) {Thread.sleep(100);}
            BagOfTasks msg2 = expectMsgAnyClassOf(BagOfTasks.class);
            assertEquals(1, msg2.getTasks().size());
            final RetrieveUrl retrieveUrl2 = msg2.getTasks().get(0);
            assertEquals("1399538",retrieveUrl2.getHeaders().get("Content-Length"));

            final ActorRef downloadAndProcess2 = RetrieveAndProcessActor.createActor(systemAndMasterActor.getKey(), httpRetrieveResponseFactory, mediaStorageClient, PATH_COLORMAP);

            // (Step 7) Hand over the first task to the slave
            downloadAndProcess2.tell(new RetrieveUrlWithProcessingConfig(retrieveUrl2, PATH_DOWNLOADED + Image1), getRef());
            while (!msgAvailable()) {
                Thread.sleep(100);
            }

            // (Step 8) Handover the response to the master
            DoneProcessing doneProcessing2 = expectMsgAnyClassOf(DoneProcessing.class);
            clusterMaster.tell(doneProcessing2, getRef());
            Thread.sleep(500);

            // (Step 9) Verify database

            final ProcessingJob processingJob2 = processingJobDao.read(doneProcessing2.getJobId());
            assertEquals(JobState.FINISHED,processingJob2.getState());

            final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics2 = sourceDocumentProcessingStatisticsDao.findBySourceDocumentReferenceAndJobId(doneProcessing2.getReferenceId(),doneProcessing2.getJobId());
            assertEquals(sourceDocumentProcessingStatistics2.getHttpResponseCode().longValue(), 200);
            assertEquals(sourceDocumentProcessingStatistics2.getHttpResponseHeaders().size(), 23);
            assertTrue(sourceDocumentProcessingStatistics2.getHttpResponseContentSizeInBytes() > 0);

            stopSystem(systemAndMasterActor.getKey());

        }};
        stopSystem(systemAndMasterActor.getKey());
    }
}
