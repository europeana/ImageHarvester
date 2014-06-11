package eu.europeana.harvester.domain;

import java.io.Serializable;

public class AudioMetaInfo implements Serializable {

    private final Integer sampleRate;

    private final Integer bitRate;

    private final Long duration;

    private final String mimeType;

    private final String fileFormat;

    public AudioMetaInfo() {
        this.sampleRate = null;
        this.bitRate = null;
        this.duration = null;
        this.mimeType = null;
        this.fileFormat = null;
    }

    public AudioMetaInfo(Integer sampleRate, Integer bitRate, Long duration, String mimeType, String fileFormat) {
        this.sampleRate = sampleRate;
        this.bitRate = bitRate;
        this.duration = duration;
        this.mimeType = mimeType;
        this.fileFormat = fileFormat;
    }

    public Integer getSampleRate() {
        return sampleRate;
    }

    public Integer getBitRate() {
        return bitRate;
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
}
