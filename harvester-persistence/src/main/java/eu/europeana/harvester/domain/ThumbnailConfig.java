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
        if (null == width || null == height) {
            throw new IllegalArgumentException("No null values are allowed");
        }

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

    @Override
    public boolean equals (Object obj) {
        if (null == obj || !(obj instanceof ThumbnailConfig)) {
            return false;
        }
        ThumbnailConfig config = (ThumbnailConfig)obj;
        return width.equals(config.getWidth()) && height.equals(config.getHeight());
    }
}
