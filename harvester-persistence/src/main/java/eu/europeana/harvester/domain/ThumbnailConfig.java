package eu.europeana.harvester.domain;

import java.io.Serializable;

/**
 * Contains all the needed information to perform the thumbnailing.
 */
public class ThumbnailConfig implements Serializable {

    /**
     * The final image width in pixels.
     */
    private Integer width;

    /**
     * The final image height in pixels.
     */
    private Integer height;

    public ThumbnailConfig(final Integer width, final Integer height) {
        this.width = width;
        this.height = height;
    }

    public ThumbnailConfig() {}

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }
}
