package eu.europeana.harvester.domain;

public class VideoMetaInfo {

    private final Integer width;

    private final Integer height;

    private final Long duration;

    private final String mimeType;

    private final String fileFormat;

    private final Double frameRate;

    public VideoMetaInfo() {
        this.width = null;
        this.height = null;
        this.duration = null;
        this.mimeType = null;
        this.fileFormat = null;
        this.frameRate = null;
    }

    public VideoMetaInfo(Integer width, Integer height, Long duration, String mimeType, String fileFormat,
                         Double frameRate) {
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
