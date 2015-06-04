package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Embedded;
import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * A specific processing job. Contains references to all links that are processed as part of the job.
 */
public class ProcessingJob {

    @Id
    @Property("id")
    private final String id;

    /** * The priority of the job; The higher the number the higher the priority.
     */
    private final Integer priority;

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

    /**
     * The hard limits on the retrieval & processing stages of the job.
     */
    private final ProcessingJobLimits limits;

    public ProcessingJob() {
        this.id = null;
        this.priority = null;
        this.expectedStartDate = null;
        this.referenceOwner = null;
        this.tasks = null;
        this.state = null;
        this.limits = new ProcessingJobLimits();
        this.ipAddress = null;
    }

    public ProcessingJob(final Integer priority, final Date expectedStartDate, final ReferenceOwner referenceOwner,
                         final List<ProcessingJobTaskDocumentReference> tasks, final JobState state, String ipAddress) {
        this.priority = priority;
        this.ipAddress = ipAddress;
        this.id = UUID.randomUUID().toString();
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
        this.limits = new ProcessingJobLimits();
    }

    public ProcessingJob(final Integer priority, final Date expectedStartDate, final ReferenceOwner referenceOwner,
                         final List<ProcessingJobTaskDocumentReference> tasks, final JobState state, String ipAddress,ProcessingJobLimits limits) {
        this.priority = priority;
        this.ipAddress = ipAddress;
        this.id = UUID.randomUUID().toString();
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
        this.limits = limits;
    }

    public ProcessingJob(final String id, final Integer priority, final Date expectedStartDate,
                         final ReferenceOwner referenceOwner, final List<ProcessingJobTaskDocumentReference> tasks,
                         final JobState state, String ipAddress,ProcessingJobLimits limits) {
        this.id = id;
        this.priority = priority;
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
        this.ipAddress = ipAddress;
        this.limits = limits;
    }

    public String getId() {
        return id;
    }

    public Integer getPriority() {
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

    public ProcessingJob withState(JobState state) {
        return new ProcessingJob(id, priority, expectedStartDate, referenceOwner, tasks, state, ipAddress, limits);
    }

    public ProcessingJob withLimits(ProcessingJobLimits limits) {
        return new ProcessingJob(id, priority, expectedStartDate, referenceOwner, tasks, state, ipAddress, limits);
    }

}
