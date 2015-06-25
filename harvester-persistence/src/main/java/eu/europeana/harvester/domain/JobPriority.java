package eu.europeana.harvester.domain;

/**
 * An enum which contains all possible priorities of a processing job.
 */
public enum JobPriority {
    /**
     * Normal
     */
    NORMAL,
    /**
     * To be processed on the fastlane
     */
    FASTLANE
}
