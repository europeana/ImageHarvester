package eu.europeana.harvester.cluster.domain;

import java.io.Serializable;

/**
 * Contains different configs for different types of processing tasks.
 * Currently there is a single type of processing task.
 */
public class JobConfigs implements Serializable {

    /**
     * Contains all the needed information to perform the thumbnailing.
     */
    private final ThumbnailConfig thumbnailConfig;

    public JobConfigs(final ThumbnailConfig thumbnailConfig) {
        this.thumbnailConfig = thumbnailConfig;
    }

    public ThumbnailConfig getThumbnailConfig() {
        return thumbnailConfig;
    }
}
