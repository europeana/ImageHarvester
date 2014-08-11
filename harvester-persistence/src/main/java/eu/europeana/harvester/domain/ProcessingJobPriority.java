package eu.europeana.harvester.domain;

/**
 * Enum which contains different levels of priorities defined for a processing job.
 */
public enum ProcessingJobPriority {

    LOW(25),
    MEDIUM(50),
    HIGH(75),
    VERY_HIGH(100);

    private final int priority;

    private ProcessingJobPriority(int priority) {
        this.priority = priority;
    }
}