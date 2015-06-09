package eu.europeana.uimtester;

import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.uimtester.domain.UIMTestSample;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by paul on 01/06/15.
 */
public class UIMTester {

    private final HarvesterClient harvesterClient;

    public UIMTester(HarvesterClient harvesterClient) {
        this.harvesterClient = harvesterClient;
    }

    public void createAndSendJobsFromSamples(final List<UIMTestSample> samples) throws MalformedURLException, UnknownHostException {

        final List<ProcessingJobTuple> generatedJobs =  generateJobs(samples);
        harvesterClient.createOrModifySourceDocumentReference(ProcessingJobTuple.sourceDocumentReferencesFromList(generatedJobs));


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
