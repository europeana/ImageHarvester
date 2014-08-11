package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

/**
 * An object which wraps all types of metainfo. It will have always maximum one field which is not null.
 */
public class SourceDocumentReferenceMetaInfo {

    @Id
    @Property("id")
    private final String id;

    /**
     * A class which contains information about an IMAGE document
     */
    private final ImageMetaInfo imageMetaInfo;

    /**
     * A class which contains information about an AUDIO document
     */
    private final AudioMetaInfo audioMetaInfo;

    /**
     * A class which contains information about a VIDEO document
     */
    private final VideoMetaInfo videoMetaInfo;

    public SourceDocumentReferenceMetaInfo() {
        this.id = null;
        this.imageMetaInfo = null;
        this.audioMetaInfo = null;
        this.videoMetaInfo = null;
    }

    public SourceDocumentReferenceMetaInfo(final String sourceDocumentReferenceId, final ImageMetaInfo imageMetaInfo,
                                           final AudioMetaInfo audioMetaInfo, final VideoMetaInfo videoMetaInfo) {
        this.id = sourceDocumentReferenceId;
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
    }

    public String getId() {
        return id;
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
