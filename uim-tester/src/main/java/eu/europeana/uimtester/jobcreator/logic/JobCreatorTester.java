package eu.europeana.uimtester.jobcreator.logic;

import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.harvester.domain.JobPriority;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.uimtester.jobcreator.domain.UIMTestSample;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class JobCreatorTester {

    private final HarvesterClient harvesterClient;
    private final JobCreatorTesterOutputWriter writer;

    public JobCreatorTester (HarvesterClient harvesterClient, JobCreatorTesterOutputWriter writer) {
        this.harvesterClient = harvesterClient;
        this.writer = writer;
    }

    public void execute(final File inputFile) throws InterruptedException, IOException,
                                                                           TimeoutException, ExecutionException {
        final List<UIMTestSample> uimTestSamples = JobCreatorTesterInputLoader.loadSamplesFromConfig(ConfigFactory.parseFile(inputFile));
        final Iterable<ProcessingJobTuple> processingJob = createAndSendJobsFromSamples(uimTestSamples);

        writer.write(processingJob);
    }

    private Iterable<ProcessingJobTuple> createAndSendJobsFromSamples(final List<UIMTestSample> samples) throws InterruptedException, IOException, TimeoutException, ExecutionException {
        final List<ProcessingJobTuple> jobTuples = generateJobs(samples);
        harvesterClient.createOrModifyProcessingJobTuples(jobTuples);
        return jobTuples;
    }

    private List<ProcessingJobTuple> generateJobs(List<UIMTestSample> samples) throws ExecutionException, IOException {
        final List<ProcessingJobTuple> generatedJobs = new ArrayList<>();
        for (final UIMTestSample testSample : samples) {
            generatedJobs.addAll(JobCreator.createJobs(
                    testSample.getCollectionId(),
                    testSample.getProviderId(),
                    testSample.getRecordId(),
                    testSample.getExecutionId(),
                    testSample.getEdmObjectUrl(),
                    testSample.getEdmHasViewUrls(),
                    testSample.getEdmIsShownByUrl(),
                    testSample.getEdmIsShownAtUrl(),
                    JobPriority.NORMAL.getPriority(),
                    new ProcessingJobCreationOptions(testSample.getForceUnconditionalDownload())
            ));
        }
        return generatedJobs;
    }

}
