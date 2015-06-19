package eu.europeana.uimtester.jobcreator.logic;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.uimtester.domain.UIMTesterFieldNames;
import eu.europeana.uimtester.jobcreator.domain.UIMTestSample;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
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
        final Iterable<com.google.code.morphia.Key<eu.europeana.harvester.domain.ProcessingJob>> processingJob = createAndSendJobsFromSamples(uimTestSamples);

        writer.write(processingJob);
    }

    private Iterable<com.google.code.morphia.Key<eu.europeana.harvester.domain.ProcessingJob>> createAndSendJobsFromSamples(final List<UIMTestSample> samples) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {

        final List<ProcessingJobTuple> generatedJobs =  generateJobs(samples);
        harvesterClient.createOrModifySourceDocumentReference(ProcessingJobTuple.sourceDocumentReferencesFromList(generatedJobs));
        return harvesterClient.createOrModify(ProcessingJobTuple.processingJobsFromList(generatedJobs));
    }

    private List<ProcessingJobTuple> generateJobs(List<UIMTestSample> samples) throws UnknownHostException, MalformedURLException {
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
                    testSample.getEdmIsShownAtUrl()
            ));
        }
        return generatedJobs;
    }

}
