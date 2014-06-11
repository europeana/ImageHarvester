package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.UUID;

public class SourceDocumentReferenceMetaInfo {

    @Id
    @Property("id")
    private final String id;

    private final String sourceDocumentReferenceId;

    private final ImageMetaInfo imageMetaInfo;

    private final AudioMetaInfo audioMetaInfo;

    private final VideoMetaInfo videoMetaInfo;

    public SourceDocumentReferenceMetaInfo() {
        this.id = null;
        this.sourceDocumentReferenceId = null;
        this.imageMetaInfo = null;
        this.audioMetaInfo = null;
        this.videoMetaInfo = null;
    }

    public SourceDocumentReferenceMetaInfo(String sourceDocumentReferenceId, ImageMetaInfo imageMetaInfo,
                                           AudioMetaInfo audioMetaInfo, VideoMetaInfo videoMetaInfo) {
        this.id = UUID.randomUUID().toString();
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
    }

    public SourceDocumentReferenceMetaInfo(String id, String sourceDocumentReferenceId, ImageMetaInfo imageMetaInfo,
                                           AudioMetaInfo audioMetaInfo, VideoMetaInfo videoMetaInfo) {
        this.id = id;
        this.sourceDocumentReferenceId = sourceDocumentReferenceId;
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
    }

    public String getId() {
        return id;
    }

    public String getSourceDocumentReferenceId() {
        return sourceDocumentReferenceId;
    }

    public ImageMetaInfo getImageMetaInfo() {
        return imageMetaInfo;
    }

    public AudioMetaInfo getAudioMetaInfo() {
        return audioMetaInfo;
    }

    public VideoMetaInfo getVideoMetaInfo() {
        return videoMetaInfo;
    }

}
