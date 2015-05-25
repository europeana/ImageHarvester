package eu.europeana.JobCreator;

import eu.europeana.JobCreator.domain.ProcessingJobCreationOptions;
import eu.europeana.JobCreator.domain.ProcessingJobTuple;
import eu.europeana.JobCreator.logic.ProcessingJobBuilder;
import eu.europeana.harvester.domain.ReferenceOwner;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;


/**
 * Handles creation of processing jobs.
 */
public class JobCreator {

    /**
     * Creates processing jobs from basic EDM object properties.
     * @param collectionId
     * @param providerId
     * @param recordId
     * @param edmObjectUrl
     * @param edmHasViewUrls
     * @param edmIsShownByUrl
     * @param edmIsShownAtUrl
     * @return
     * @throws UnknownHostException
     * @throws MalformedURLException
     */
    public final static List<ProcessingJobTuple> createJobs(final String collectionId,
                                                            final String providerId,
                                                            final String recordId,
                                                            final String edmObjectUrl,
                                                            final List<String> edmHasViewUrls,
                                                            final String edmIsShownByUrl,
                                                            final String edmIsShownAtUrl
                                                            ) throws UnknownHostException, MalformedURLException {
        return createJobs(collectionId,providerId,recordId,edmObjectUrl,edmHasViewUrls,edmIsShownByUrl,edmIsShownAtUrl,new ProcessingJobCreationOptions(false));
    }

    /**
     * Creates processing jobs from basic EDM object properties.
     * @param collectionId
     * @param providerId
     * @param recordId
     * @param edmObjectUrl
     * @param edmHasViewUrls
     * @param edmIsShownByUrl
     * @param edmIsShownAtUrl
     * @param options
     * @return
     * @throws UnknownHostException
     * @throws MalformedURLException
     */
    public final static List<ProcessingJobTuple> createJobs(final String collectionId,
                                                     final String providerId,
                                                     final String recordId,
                                                     final String edmObjectUrl,
                                                     final List<String> edmHasViewUrls,
                                                     final String edmIsShownByUrl,
                                                     final String edmIsShownAtUrl,
                                                     final ProcessingJobCreationOptions options) throws UnknownHostException, MalformedURLException {

        if (null == collectionId || null == providerId || null == recordId) {
            throw new IllegalArgumentException("Incomplete ownership information : collectionId = " + collectionId + ", providerId = " + providerId + " and recordId = " + recordId + ". Neither of them can be null. ");
        }

        if (null == options) {
          throw new IllegalArgumentException("Options cannot be null");
        }

        final List<ProcessingJobTuple> results = new ArrayList();
        final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId);

        if (null != edmObjectUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(edmObjectUrl, owner, options));
        }

        if (null != edmHasViewUrls && !edmHasViewUrls.isEmpty()) {
            results.addAll(ProcessingJobBuilder.edmHasViewUrlsJobs(edmHasViewUrls, owner, options));
        }

        if (null != edmIsShownByUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(edmIsShownByUrl, owner, options));
        }

        if (null != edmIsShownAtUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownAtUrlJobs(edmIsShownAtUrl, owner, options));

        }

        return results;
    }
}
