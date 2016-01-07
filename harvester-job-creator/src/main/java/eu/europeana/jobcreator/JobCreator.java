package eu.europeana.jobcreator;

import eu.europeana.harvester.domain.DocumentReferenceTaskType;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.URLSourceType;
import eu.europeana.harvester.util.CachingUrlResolver;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import eu.europeana.jobcreator.logic.ProcessingJobBuilder;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;


/**
 * Handles creation of processing jobs.
 */
public class JobCreator {

    public final static CachingUrlResolver URL_RESOLVER = new CachingUrlResolver();

    public final static List<ProcessingJobTuple> createJobs (final ReferenceOwner owner,
                                                             final SourceDocumentReference reference,
                                                             final URLSourceType urlSourceType,
                                                             final int priority,
                                                             final DocumentReferenceTaskType taskType
                                                            ) throws ExecutionException {

        final String url = reference.getUrl();
        final String edmObjectUrl = (URLSourceType.OBJECT == urlSourceType) ? url : null;
        final List<String> edmHasViewUrl = (URLSourceType.HASVIEW == urlSourceType) ? Arrays.asList(url) : null;
        final String edmIsShownByUrl = (URLSourceType.ISSHOWNBY == urlSourceType) ? url: null;
        final String edmIshShownAtUrl = (URLSourceType.ISSHOWNAT == urlSourceType) ? url: null;
        final ProcessingJobCreationOptions options = new ProcessingJobCreationOptions(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD == taskType);

        return createJobs(owner.getCollectionId(),
                          owner.getProviderId(),
                          owner.getRecordId(),
                          owner.getExecutionId(),
                          edmObjectUrl,
                          edmHasViewUrl,
                          edmIsShownByUrl,
                          edmIshShownAtUrl,
                          priority,
                          reference,
                          options
                          );
    }

    /**
     * Creates processing jobs from basic EDM object properties.
     *
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
                                                            final String executionId,
                                                            final String edmObjectUrl,
                                                            final List<String> edmHasViewUrls,
                                                            final String edmIsShownByUrl,
                                                            final String edmIsShownAtUrl,
                                                            final Integer priority
                                                            ) throws ExecutionException {
        return createJobs(collectionId, providerId, recordId, executionId, edmObjectUrl, edmHasViewUrls, edmIsShownByUrl, edmIsShownAtUrl, priority, new ProcessingJobCreationOptions(false));
    }

    /**
     * Creates processing jobs from basic EDM object properties.
     *
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
                                                            final String executionId,
                                                            final String edmObjectUrl,
                                                            final List<String> edmHasViewUrls,
                                                            final String edmIsShownByUrl,
                                                            final String edmIsShownAtUrl,
                                                            final Integer priority,
                                                            final ProcessingJobCreationOptions options) throws ExecutionException {

        if (null == collectionId || null == providerId || null == recordId) {
            throw new IllegalArgumentException("Incomplete ownership information : collectionId = " + collectionId + ", providerId = " + providerId + " and recordId = " + recordId + ". Neither of them can be null. ");
        }

        if (null == options) {
            throw new IllegalArgumentException("Options cannot be null");
        }

        final List<ProcessingJobTuple> results = new ArrayList();
        final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId, executionId);

        if (null != edmObjectUrl) {
            results.addAll(ProcessingJobBuilder.edmObjectUrlJobs(edmObjectUrl, owner,priority, options));
        }

        if (null != edmHasViewUrls && !edmHasViewUrls.isEmpty()) {
            results.addAll(ProcessingJobBuilder.edmHasViewUrlsJobs(edmHasViewUrls, owner,priority, options));
        }

        if (null != edmIsShownByUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(edmIsShownByUrl, owner,priority, options));
        }

        if (null != edmIsShownAtUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownAtUrlJobs(edmIsShownAtUrl, owner,priority, options));

        }

        return results;
    }

    /**
     * Creates processing jobs from basic EDM object properties.
     *
     * @param collectionId
     * @param providerId
     * @param recordId
     * @param edmObjectUrl
     * @param edmHasViewUrls
     * @param edmIsShownByUrl
     * @param edmIsShownAtUrl
     * @param sourceDocumentReference
     * @param options
     * @return
     * @throws UnknownHostException
     * @throws MalformedURLException
     */
    public final static List<ProcessingJobTuple> createJobs(final String collectionId,
                                                            final String providerId,
                                                            final String recordId,
                                                            final String executionId,
                                                            final String edmObjectUrl,
                                                            final List<String> edmHasViewUrls,
                                                            final String edmIsShownByUrl,
                                                            final String edmIsShownAtUrl,
                                                            final Integer priority,
                                                            final SourceDocumentReference sourceDocumentReference,
                                                            final ProcessingJobCreationOptions options) throws ExecutionException {

        if (null == collectionId || null == providerId || null == recordId) {
            throw new IllegalArgumentException("Incomplete ownership information : collectionId = " + collectionId + ", providerId = " + providerId + " and recordId = " + recordId + ". Neither of them can be null. ");
        }

        if (null == options) {
            throw new IllegalArgumentException("Options cannot be null");
        }

        final List<ProcessingJobTuple> results = new ArrayList();
        final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId, executionId);

        if (null != edmObjectUrl) {
            results.addAll(ProcessingJobBuilder.edmObjectUrlJobs(edmObjectUrl, owner,priority, sourceDocumentReference, options));
        }

        if (null != edmHasViewUrls && !edmHasViewUrls.isEmpty()) {
            results.addAll(ProcessingJobBuilder.edmHasViewUrlsJobs(edmHasViewUrls, owner,priority, sourceDocumentReference, options));
        }

        if (null != edmIsShownByUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownByUrlJobs(edmIsShownByUrl, owner,priority, sourceDocumentReference, options));
        }

        if (null != edmIsShownAtUrl) {
            results.addAll(ProcessingJobBuilder.edmIsShownAtUrlJobs(edmIsShownAtUrl, owner,priority, sourceDocumentReference, options));
        }

        return results;
    }
}
