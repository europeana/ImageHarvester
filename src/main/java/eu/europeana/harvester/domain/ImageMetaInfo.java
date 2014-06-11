package eu.europeana.harvester.domain;

import java.io.Serializable;

public class ImageMetaInfo implements Serializable {

    private final Integer width;

    private final Integer height;

    private final String mimeType;

    private final String fileFormat;

    private final String colorSpace;

    public ImageMetaInfo() {
        this.width = null;
        this.height = null;
        this.mimeType = null;
        this.fileFormat = null;
        this.colorSpace = null;
    }

    public ImageMetaInfo(Integer width, Integer height, String mimeType, String fileFormat, String colorSpace) {
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
