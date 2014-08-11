package eu.europeana.harvester.cluster.domain;

import java.io.Serializable;

/**
 * Contains all the needed information to perform the thumbnailing.
 */
public class ThumbnailConfig implements Serializable {

    /**
     * The final image width in pixels.
     */
    private final Integer width;

    /**
     * The final image height in pixels.
     */
    private final Integer height;

    public ThumbnailConfig(final Integer width, final Integer height) {
        this.width = width;
        this.height = height;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }
}
