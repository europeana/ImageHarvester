package eu.europeana.harvester.domain;

import java.io.Serializable;

/**
 * Contains different configs for different types of processing tasks.
 * Currently there is a single type of processing task.
 */
public class GenericSubTaskConfiguration implements Serializable {

    /**
     * The thumbnail config object for the processer actor
     */
    private ThumbnailConfig thumbnailConfig;

    public GenericSubTaskConfiguration(ThumbnailConfig thumbnailConfig) {
        this.thumbnailConfig = thumbnailConfig;
    }

    public GenericSubTaskConfiguration() {}

    public ThumbnailConfig getThumbnailConfig() {
        return thumbnailConfig;
    }

    @Override
    public boolean equals (Object obj) {
        if (null == obj || !(obj instanceof GenericSubTaskConfiguration)) {
            return false;
        }
        GenericSubTaskConfiguration config = (GenericSubTaskConfiguration)obj;
        return null == thumbnailConfig ? null == config.getThumbnailConfig() :
                                         thumbnailConfig.equals(config.getThumbnailConfig());
    }
}
