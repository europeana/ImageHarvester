package eu.europeana.harvester.domain;

import java.io.Serializable;

/**
 * A class which contains information about an IMAGE document
 */
public class ImageMetaInfo implements Serializable {

    /**
     * The width of image in pixels.
     */
    private final Integer width;

    /**
     * The height of image in pixels.
     */
    private final Integer height;

    /**
     * An Internet media type is a standard identifier used on the
     * Internet to indicate the type of data that a file contains.
     */
    private final String mimeType;

    /**
     * A file format is a standard way that information is encoded for storage in a computer file.
     */
    private final String fileFormat;

    /**
     * A color space is a specific organization of colors.
     */
    private final String colorSpace;

    public ImageMetaInfo() {
        this.width = null;
        this.height = null;
        this.mimeType = null;
        this.fileFormat = null;
        this.colorSpace = null;
    }

    public ImageMetaInfo(final Integer width, final Integer height,
                         final String mimeType, final String fileFormat, final String colorSpace) {
        this.width = width;
        this.height = height;
        this.mimeType = mimeType;
        this.fileFormat = fileFormat;
        this.colorSpace = colorSpace;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public String getColorSpace() {
        return colorSpace;
    }

}
