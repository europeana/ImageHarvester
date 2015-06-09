package eu.europeana.uimtester.jobcreator.logic;

import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.uimtester.jobcreator.domain.UIMTestSample;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class JobCreatorTester {

    private final HarvesterClient harvesterClient;

    public JobCreatorTester(HarvesterClient harvesterClient) {
        this.harvesterClient = harvesterClient;
    }

    public Iterable<com.google.code.morphia.Key<eu.europeana.harvester.domain.ProcessingJob>> createAndSendJobsFromSamples(final List<UIMTestSample> samples) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {

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
