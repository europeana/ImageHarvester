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

    public ProcessingJob() {
        this.id = null;
        this.expectedStartDate = null;
        this.referenceOwner = null;
        this.tasks = null;
        this.state = null;
    }

    public ProcessingJob(Date expectedStartDate, ReferenceOwner referenceOwner,
                         List<ProcessingJobTaskDocumentReference> tasks, JobState state) {
        this.id = UUID.randomUUID().toString();
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
    }

    public ProcessingJob(String id, Date expectedStartDate, ReferenceOwner referenceOwner,
                         List<ProcessingJobTaskDocumentReference> tasks, JobState state) {
        this.id = id;
        this.expectedStartDate = expectedStartDate;
        this.referenceOwner = referenceOwner;
        this.tasks = tasks;
        this.state = state;
    }

    public String getId() {
        return id;
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

    public ProcessingJob withState(JobState state) {
        return new ProcessingJob(id, expectedStartDate, referenceOwner, tasks, state);
    }

}
