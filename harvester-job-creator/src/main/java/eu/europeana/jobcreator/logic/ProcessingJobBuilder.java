package eu.europeana.jobcreator.logic;

import eu.europeana.harvester.domain.*;
import eu.europeana.jobcreator.JobCreator;
import eu.europeana.jobcreator.domain.ProcessingJobCreationOptions;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Builder for various types of processing jobs.
 */
public class ProcessingJobBuilder {

    private static final String PDF_FILE_EXTENSION = "pdf";

    /**
     * Decides the type of the document task type depending on the processing options.
     * @param options
     * @return
     */
    private static DocumentReferenceTaskType documentReferenceTaskTypeFromOptions(final ProcessingJobCreationOptions options) {
        return (options.isForceUnconditionalDownload()) ?
                DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD :
                DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD;
    }

    /**
     * Creates a processing job and it's source reference document from a EDM Object URL.
     * @param url
     * @param owner
     * @param options
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     * @throws IOException
     */
    public static final List<ProcessingJobTuple> edmObjectUrlJobs(final String url, final ReferenceOwner owner, final Integer priority,
                                                                  final ProcessingJobCreationOptions options) throws ExecutionException, IOException {
        if (null == options) {
            throw new IllegalArgumentException("options must not be null");
        }

        final SourceDocumentReference sourceDocumentReference = new SourceDocumentReference(owner, url, null, null, null, null, true);

        final List<ProcessingJobSubTask> subTasks = getSubTasks(sourceDocumentReference.getUrl());

        final ProcessingJob processingJob = new ProcessingJob(priority, new Date(), owner,
                Arrays.asList(
                        new ProcessingJobTaskDocumentReference(documentReferenceTaskTypeFromOptions(options),
                                sourceDocumentReference.getId(),
                                subTasks)
                ),
                JobState.READY,
                URLSourceType.OBJECT,
                JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url),
                true);

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles =
               Arrays.asList(
                  SourceDocumentReferenceProcessingProfileBuilder.edmObjectUrl(sourceDocumentReference.getId(),
                                                                               owner,
                                                                               priority
                                                                              )
               );

        return Arrays.asList(
                new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
    }

    /**
     * Creates a processing job and it's source reference document from a EDM Has View URL.
     * @param urls
     * @param owner
     * @param options
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     * @throws IOException
     */
    public static final List<ProcessingJobTuple> edmHasViewUrlsJobs(final List<String> urls, final ReferenceOwner owner, final Integer priority,
                                                                    final ProcessingJobCreationOptions options) throws ExecutionException, IOException {
        if (null == options) {
            throw new IllegalArgumentException("options must not be null");
        }

        final List<ProcessingJobTuple> results = new ArrayList();
        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles = new ArrayList<>();
        for (final String url : urls) {

            final SourceDocumentReference sourceDocumentReference = new SourceDocumentReference(owner, url, null, null, null, null, true);

            final List<ProcessingJobSubTask> subTasks = getSubTasks(sourceDocumentReference.getUrl());

            final ProcessingJob processingJob = new ProcessingJob(priority, new Date(), owner,
                    Arrays.asList(
                            new ProcessingJobTaskDocumentReference(documentReferenceTaskTypeFromOptions(options),
                                    sourceDocumentReference.getId(),
                                    subTasks)
                    ),
                    JobState.READY, URLSourceType.HASVIEW, JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url), true);

            sourceDocumentReferenceProcessingProfiles.add(
                SourceDocumentReferenceProcessingProfileBuilder.edmHasView(sourceDocumentReference.getId(),
                                                                           owner,
                                                                           priority
                                                                          )
            );

            results.add(
                    new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
        }
        return results;
    }

    /**
     *  Creates a processing job and it's source reference document from a EDM IS SHOWN BY URL.
     * @param url
     * @param owner
     * @param options
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     * @throws IOException
     */
    public static final List<ProcessingJobTuple> edmIsShownByUrlJobs(final String url, final ReferenceOwner owner, final Integer priority,
                                                                     final ProcessingJobCreationOptions options) throws ExecutionException, IOException {
        if (null == options) {
            throw new IllegalArgumentException("options must not be null");
        }

        final SourceDocumentReference sourceDocumentReference = new SourceDocumentReference(owner, url, null, null, null, null, true);

        final List<ProcessingJobSubTask> subTasks = getSubTasks(sourceDocumentReference.getUrl());

        final ProcessingJob processingJob = new ProcessingJob(priority, new Date(), owner,
                Arrays.asList(
                        new ProcessingJobTaskDocumentReference(documentReferenceTaskTypeFromOptions(options),
                                sourceDocumentReference.getId(),
                                subTasks)
                ),
                JobState.READY, URLSourceType.ISSHOWNBY, JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url), true);

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles =
                Arrays.asList (
                   SourceDocumentReferenceProcessingProfileBuilder.edmIsShownBy (sourceDocumentReference.getId(),
                                                                                 owner,
                                                                                 priority
                                                                                )
                );

        return Arrays.asList(
                new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
    }


    /**
     *  Creates a processing job and it's source reference document from a EDM IS SHOWN AT URL.
     * @param url
     * @param owner
     * @param options
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     */
    public static final List<ProcessingJobTuple> edmIsShownAtUrlJobs(final String url, final ReferenceOwner owner, final Integer priority,
                                                                     final ProcessingJobCreationOptions options) throws ExecutionException {
        if (null == options) {
            throw new IllegalArgumentException("options must not be null");
        }

        final SourceDocumentReference sourceDocumentReference = new SourceDocumentReference(owner, url, null, null, null, null, true);

        final ProcessingJob processingJob = new ProcessingJob(priority, new Date(), owner,
                Arrays.asList(
                        new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                                sourceDocumentReference.getId(),
                                new ArrayList())
                ),
                JobState.READY, URLSourceType.ISSHOWNAT, JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url), true);

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles =
            Arrays.asList (
                 SourceDocumentReferenceProcessingProfileBuilder.edmIsShownAt(sourceDocumentReference.getId(),
                                                                              owner,
                                                                              priority
                                                                             )
            );
        return Arrays.asList(
                new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
    }



    /**
     * Creates a processing job and it's source reference document from a EDM Object URL.
     * @param url
     * @param owner
     * @param options
     * @param sourceDocumentReference
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     * @throws IOException
     */
    public static final List<ProcessingJobTuple> edmObjectUrlJobs(final String url, final ReferenceOwner owner, final Integer priority,
                                                                  final SourceDocumentReference sourceDocumentReference,
                                                                  final ProcessingJobCreationOptions options) throws ExecutionException, IOException {
        if (null == options || sourceDocumentReference == null) {
            throw new IllegalArgumentException("options/sourceDocumentReference must not be null");
        }

        final List<ProcessingJobSubTask> subTasks = new ArrayList();

        // exclude COLOR EXTRACTION for PDF documents
        if (!FilenameUtils.getExtension(url).contains(PDF_FILE_EXTENSION)) {
            subTasks.addAll(SubTaskBuilder.colourExtraction());
        }
        subTasks.addAll(SubTaskBuilder.thumbnailGeneration());

        final ProcessingJob processingJob = new ProcessingJob(
              priority,
              new Date(),
              owner,
              Arrays.asList(new ProcessingJobTaskDocumentReference(documentReferenceTaskTypeFromOptions(options),
                                                                   sourceDocumentReference.getId(),
                                                                   subTasks)
                            ),
              JobState.READY,
              URLSourceType.OBJECT,
                JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url),
              true
        );

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles = Arrays.asList(
              SourceDocumentReferenceProcessingProfileBuilder.edmObjectUrl(sourceDocumentReference.getId(),
                                                                           owner,
                                                                           priority
                                                                          )
        );

        return Arrays.asList(new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
    }

    /**
     * Creates a processing job and it's source reference document from a EDM Has View URL.
     * @param urls
     * @param owner
     * @param options
     * @param sourceDocumentReference
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     * @throws IOException
     */
    public static final List<ProcessingJobTuple> edmHasViewUrlsJobs(final List<String> urls,
                                                                    final ReferenceOwner owner,
                                                                    final Integer priority,
                                                                    final SourceDocumentReference sourceDocumentReference,
                                                                    final ProcessingJobCreationOptions options) throws ExecutionException, IOException {
        if (null == options || null == sourceDocumentReference) {
            throw new IllegalArgumentException("options must not be null");
        }

        final List<ProcessingJobTuple> results = new ArrayList();
        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles = new ArrayList<>();
        for (final String url : urls) {
            final List<ProcessingJobSubTask> subTasks = getSubTasks(sourceDocumentReference.getUrl());

            final ProcessingJob processingJob = new ProcessingJob (
                  priority,
                  new Date(),
                  owner,
                  Arrays.asList(new ProcessingJobTaskDocumentReference(documentReferenceTaskTypeFromOptions(options),
                                                                       sourceDocumentReference.getId(),
                                                                       subTasks
                                                                      )
                                ),
                  JobState.READY,
                  URLSourceType.HASVIEW,
                    JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url),
                  true
            );

            sourceDocumentReferenceProcessingProfiles.add(
                  SourceDocumentReferenceProcessingProfileBuilder.edmHasView(sourceDocumentReference.getId(),
                                                                             owner,
                                                                             priority
                                                                            )
                  );

            results.add(new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
        }
        return results;
    }

    /**
     *  Creates a processing job and it's source reference document from a EDM IS SHOWN BY URL.
     * @param url
     * @param owner
     * @param options
     * @param sourceDocumentReference
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     * @throws IOException
     */
    public static final List<ProcessingJobTuple> edmIsShownByUrlJobs(final String url,
                                                                     final ReferenceOwner owner,
                                                                     final Integer priority,
                                                                     final SourceDocumentReference sourceDocumentReference,
                                                                     final ProcessingJobCreationOptions options) throws ExecutionException, IOException {
        if (null == options || null == sourceDocumentReference) {
            throw new IllegalArgumentException("options/sourceDocumentReference must not be null");
        }

        final List<ProcessingJobSubTask> subTasks = getSubTasks(sourceDocumentReference.getUrl());
        final ProcessingJob processingJob = new ProcessingJob(
              priority,
              new Date(),
              owner,
              Arrays.asList(new ProcessingJobTaskDocumentReference(documentReferenceTaskTypeFromOptions(options),
                                                                   sourceDocumentReference.getId(),
                                                                   subTasks
                                                                  )
                           ),
              JobState.READY,
              URLSourceType.ISSHOWNBY,
                JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url),
              true
        );

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles = Arrays.asList (
             SourceDocumentReferenceProcessingProfileBuilder.edmIsShownBy (sourceDocumentReference.getId(),
                                                                           owner,
                                                                           priority
                                                                          )
             );

        return Arrays.asList(new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
    }


    /**
     *  Creates a processing job and it's source reference document from a EDM IS SHOWN AT URL.
     * @param url
     * @param owner
     * @param options
     * @param sourceDocumentReference
     * @return
     * @throws MalformedURLException
     * @throws UnknownHostException
     */
    public static final List<ProcessingJobTuple> edmIsShownAtUrlJobs(final String url,
                                                                     final ReferenceOwner owner,
                                                                     final Integer priority,
                                                                     final SourceDocumentReference sourceDocumentReference,
                                                                     final ProcessingJobCreationOptions options) throws ExecutionException {
        if (null == options || sourceDocumentReference == null) {
            throw new IllegalArgumentException("options/sourceDocumentReference must not be null");
        }

        final ProcessingJob processingJob = new ProcessingJob(
              priority,
              new Date(),
              owner,
              Arrays.asList(new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK,
                                                                   sourceDocumentReference.getId(),
                                                                   new ArrayList()
                                                                  )
                            ),
              JobState.READY,
              URLSourceType.ISSHOWNAT,
                JobCreator.URL_RESOLVER.resolveIpOfUrlAndReturnLoopbackOnFail(url),
              true);

        final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles = Arrays.asList (
              SourceDocumentReferenceProcessingProfileBuilder.edmIsShownAt(sourceDocumentReference.getId(),
                                                                           owner,
                                                                           priority
                                                                          )
         );

        return Arrays.asList(new ProcessingJobTuple(processingJob, sourceDocumentReference,sourceDocumentReferenceProcessingProfiles));
    }

    /**
     * Gets all the subtasks a processing job uses, depending on document type
     * @param url of the source reference to check mime type of the document
     * @return the list of specific subtasks
     * @throws IOException
     */
    private static final List<ProcessingJobSubTask> getSubTasks(final String url) throws IOException {
        final List<ProcessingJobSubTask> subTasks = new ArrayList();

        // exclude COLOR EXTRACTION for PDF files
        if (!FilenameUtils.getExtension(url).contains(PDF_FILE_EXTENSION)) {
            subTasks.addAll(SubTaskBuilder.colourExtraction());
        }
        subTasks.addAll(SubTaskBuilder.thumbnailGeneration());
        subTasks.addAll(SubTaskBuilder.metaExtraction());

        return subTasks;
    }
}
