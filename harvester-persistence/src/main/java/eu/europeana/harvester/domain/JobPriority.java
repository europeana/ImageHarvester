package eu.europeana.harvester.domain;

/**
 * An enum which contains all possible priorities of a processing job.
 */
public enum JobPriority {
    /**
     * Normal
     */
    NORMAL(0),
    /**
     * To be processed on the fastlane
     */
    FASTLANE(1);


    private final int priority;

    private JobPriority(int priority) {
        this.priority = priority;
    }

    public int getPriority() {
        return priority;
    }

    public static JobPriority fromPriority(Integer priorityValue) {
        for (final JobPriority prio : values()) {
            if (prio.getPriority() == priorityValue) return prio;
        }
        return null;
    }
}
