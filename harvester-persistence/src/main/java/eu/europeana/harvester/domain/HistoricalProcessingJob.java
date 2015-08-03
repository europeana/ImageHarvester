package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * A finished processing job. Contains references to all links that are processed as part of the job.
 */
public class HistoricalProcessingJob {

    @Id
    @Property("id")
    private final String id;

    /** * The priority of the job; The higher the number the higher the priority.
     */
    private final int priority;

    /**
     * The expected start date.
     */
    private final Date expectedStartDate;

    /**
     * An object which contains: provider id, collection id, record id
     */
    private final ReferenceOwner referenceOwner;

    /**
     * The tasks that have to be executed in the processing job.
     * TODO: make this a single object, not a list
     */
    @Embedded
    private final List<ProcessingJobTaskDocumentReference> tasks;

    /**
     * The state of the processing job. Indicates an aggregate state of all the links in the job.
     */
    private final JobState state;

    /**
     * The IP address of the host machine.
     */
    private final String ipAddress;

    private final Boolean active;

    /**
     * The hard limits on the retrieval & processing stages of the job.
     */
    private final ProcessingJobLimits limits;

    private final URLSourceType urlSourceType;

    public HistoricalProcessingJob() {
        this.id = null;
        this.priority = 0;
        this.expectedStartDate = null;
        this.referenceOwner = null;
        this.tasks = null;
        this.state = null;
        this.limits = new ProcessingJobLimits();
        this.ipAddress = null;
        this.active = null;
        this.urlSourceType = null;
    }

    public HistoricalProcessingJob(final int priority,
                                   final Date expectedStartDate,
                                   final ReferenceOwner referenceOwner,
                                   final List<ProcessingJobTaskDocumentReference> tasks,
                                   final JobState state,
                                   final URLSourceType urlSourceType,
                                   final String ipAddress,
                                   final Boolean active) {
        this.priority = priority;
        this.urlSourceType = urlSourceType;
        this.ipAddress = ipAddress;
        this.active = active;
        this.id = UUID.randomUUID().toString();
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
        this.limits = new ProcessingJobLimits();
    }

    public HistoricalProcessingJob(final int priority,
                                   final Date expectedStartDate,
                                   final ReferenceOwner referenceOwner,
                                   final List<ProcessingJobTaskDocumentReference> tasks,
                                   final JobState state,
                                   final URLSourceType urlSourceType,
                                   final String ipAddress,
                                   final Boolean active,
                                   final ProcessingJobLimits limits) {
        this.priority = priority;
        this.urlSourceType = urlSourceType;
        this.ipAddress = ipAddress;
        this.active = active;
        this.id = UUID.randomUUID().toString();
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
        this.limits = limits;
    }

    public HistoricalProcessingJob(final String id,
                                   final int priority,
                                   final Date expectedStartDate,
                                   final ReferenceOwner referenceOwner,
                                   final List<ProcessingJobTaskDocumentReference> tasks,
                                   final JobState state,
                                   final URLSourceType urlSourceType,
                                   String ipAddress,
                                   Boolean active,
                                   ProcessingJobLimits limits) {
        this.id = id;
        this.priority = priority;
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
        this.urlSourceType = urlSourceType;
        this.ipAddress = ipAddress;
        this.limits = limits;
        this.active = active;
    }

    public String getId() {
        return id;
    }

    public int getPriority() {
        return priority;
    }

    public Date getExpectedStartDate() {
        return expectedStartDate;
    }

    public ReferenceOwner getReferenceOwner() {
        return referenceOwner;
    }

    public List<ProcessingJobTaskDocumentReference> getTasks() {
        return tasks;
    }

    public JobState getState() {
        return state;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public ProcessingJobLimits getLimits() {
        if (limits == null) return new ProcessingJobLimits(); /* Repair on Read if missing */
        else return limits;
    }

    public URLSourceType getUrlSourceType() {return urlSourceType;}

    public HistoricalProcessingJob withState(JobState state) {
        return new HistoricalProcessingJob(id, priority, expectedStartDate, referenceOwner, tasks, state, urlSourceType, ipAddress, active,
                                 limits);
    }

    public HistoricalProcessingJob withLimits(ProcessingJobLimits limits) {
        return new HistoricalProcessingJob(id, priority, expectedStartDate, referenceOwner, tasks, state, urlSourceType, ipAddress, active,
                                 limits);
    }

    public Boolean getActive() {return  active;}

    public List<String> getAllReferencedSourceDocumentIds() {
        final List<String> results = new ArrayList<String>();
        for (final ProcessingJobTaskDocumentReference task : this.tasks) {
            results.add(task.getSourceDocumentReferenceID());
        }
        return results;
    }
}
