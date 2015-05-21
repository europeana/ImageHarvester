package eu.europeana.JobCreator;

import eu.europeana.harvester.domain.*;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by salexandru on 18.05.2015.
 */
public class JobCreator {
    private List<SourceDocumentReference> sourceDocumentReferences;

    public JobCreator() {
        sourceDocumentReferences = new ArrayList<>();
    }

    /**
     * Gets the ip address of a source document.
     * @param url the resource url
     * @return - ip address
     */
    private String getIPAddress(final String url) throws UnknownHostException, MalformedURLException {
        return InetAddress.getByName(new URL(url).getHost()).getHostAddress();
    }

    private ProcessingJob createJob (ReferenceOwner owner,
                                     String url,
                                     URLSourceType urlSourceType,
                                     ProcessingJobSubTaskType[] taskTypes,
                                     DocumentReferenceTaskType jobType
                                    ) throws UnknownHostException, MalformedURLException {
        final SourceDocumentReference sourceDocumentReference = new SourceDocumentReference(owner, urlSourceType, url,
                                                                                            null, null, 0L, null, true
                                                                                           );
        sourceDocumentReferences.add(sourceDocumentReference);

        final List<ProcessingJobSubTask> subTasks = new ArrayList<>();

        for (final ProcessingJobSubTaskType taskType: taskTypes) {
            switch (taskType) {
                case COLOR_EXTRACTION:
                case META_EXTRACTION:
                    subTasks.add(new ProcessingJobSubTask(taskType, null));
                    break;

                case GENERATE_THUMBNAIL:
                    subTasks.add(new ProcessingJobSubTask(taskType, new GenericSubTaskConfiguration(new ThumbnailConfig(180, 180))));
                    subTasks.add(new ProcessingJobSubTask(taskType, new GenericSubTaskConfiguration(new ThumbnailConfig(200, 200))));
                    break;

                default: throw new UnsupportedOperationException("Unknown processing job subtask type: " + taskType.name());
            }
        }

        final ProcessingJobTaskDocumentReference jobTaskDocumentReference =
                new ProcessingJobTaskDocumentReference(jobType,
                                                       sourceDocumentReference.getId(),
                                                       subTasks);



        return new ProcessingJob(1, new Date(), owner, Arrays.asList(jobTaskDocumentReference), JobState.READY, getIPAddress(url));
    }

    public List<SourceDocumentReference> getSourceDocumentReferences() {
        return sourceDocumentReferences;
    }

    public List<ProcessingJob> createJobs(String collectionId,
                                          String providerId,
                                          String recordId,
                                          String edmObjectUrl,
                                          List<String> edmHasViewUrls,
                                          String edmIsShownByUrl,
                                          String edmIsShownAtUrl,
                                          ProcessingJobCreationOptions options) throws UnknownHostException, MalformedURLException {


        if (null == collectionId || null == providerId || null == recordId) {
            throw new NullPointerException("collectionId, providerId and recordId cannot be null!");
        }

        sourceDocumentReferences.clear();
        sourceDocumentReferences = new ArrayList<SourceDocumentReference>();

        final List<ProcessingJob> jobs = new ArrayList<>();
        final DocumentReferenceTaskType taskType = ProcessingJobCreationOptions.FORCE_UNCONDITIONAL_DOWNLOAD.equals(options) ?
                                                   DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD :
                                                   DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD;

        if (null != edmObjectUrl) {
            final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId);
            jobs.add(createJob(owner,
                               edmObjectUrl,
                               URLSourceType.ISSHOWNBY,
                               new ProcessingJobSubTaskType[] {ProcessingJobSubTaskType.COLOR_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL},
                               taskType
                              )
                    );
        }

        if (null != edmHasViewUrls) {
            final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId);

            for (final String url: edmHasViewUrls) {
                jobs.add(createJob(owner,
                                   url,
                                   URLSourceType.HASVIEW,
                                   new ProcessingJobSubTaskType[] {ProcessingJobSubTaskType.META_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL, ProcessingJobSubTaskType.COLOR_EXTRACTION},
                                   taskType
                                  )
                        );
            }
        }

        if (null != edmIsShownByUrl) {
            final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId);
            jobs.add(createJob(owner,
                               edmIsShownByUrl,
                               URLSourceType.ISSHOWNBY,
                               new ProcessingJobSubTaskType[] {ProcessingJobSubTaskType.META_EXTRACTION, ProcessingJobSubTaskType.GENERATE_THUMBNAIL, ProcessingJobSubTaskType.COLOR_EXTRACTION},
                               taskType
                              )
                    );
        }

        if (null != edmIsShownAtUrl) {
            final ReferenceOwner owner = new ReferenceOwner(providerId, collectionId, recordId);
            jobs.add(createJob(owner,
                               edmIsShownAtUrl,
                               null,
                               new ProcessingJobSubTaskType[] {},
                               DocumentReferenceTaskType.CHECK_LINK
                              )
                    );
        }

        return jobs;
    }
}
