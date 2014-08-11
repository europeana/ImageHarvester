package eu.europeana.harvester.domain;

import java.io.Serializable;

/**
 * A class which contains information about a VIDEO document
 */
public class VideoMetaInfo implements Serializable {

    /**
     * The width of frames in pixels.
     */
    private final Integer width;

    /**
     * The height of frames in pixels.
     */
    private final Integer height;

    /**
     * The video document duration in milliseconds.
     */
    private final Long duration;

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
     *  also known as frame frequency and frames per second (FPS), is the frequency (rate)
     *  at which an imaging device produces unique consecutive images called frames.
     */
    private final Double frameRate;

    public VideoMetaInfo() {
        this.width = null;
        this.height = null;
        this.duration = null;
        this.mimeType = null;
        this.fileFormat = null;
        this.frameRate = null;
    }

    public VideoMetaInfo(final Integer width, final Integer height, final Long duration, final String mimeType,
                         final String fileFormat, final Double frameRate) {
        this.width = width;
        this.height = height;
        this.duration = duration;
        this.mimeType = mimeType;
        this.fileFormat = fileFormat;
        this.frameRate = frameRate;
    }

    public Integer getWidth() {
        return width;
    }

    public Integer getHeight() {
        return height;
    }

    public Long getDuration() {
        return duration;
    }

    public String getMimeType() {
        return mimeType;
    }

    public String getFileFormat() {
        return fileFormat;
    }

    public Double getFrameRate() {
        return frameRate;
    }

}
