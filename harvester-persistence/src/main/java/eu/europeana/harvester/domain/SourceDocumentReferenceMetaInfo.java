package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;
import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * An object which wraps all types of metainfo. It will have always maximum one field which is not null.
 */
public class SourceDocumentReferenceMetaInfo {

    public static final String idFromUrl(final String url) {
        final HashFunction hf = Hashing.md5();
        final HashCode hc = hf.newHasher()
                .putString(url, Charsets.UTF_8)
                .hash();
        return hc.toString();
    }

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

    /**
     * A class which contains information about a TEXT document
     */
    private final TextMetaInfo textMetaInfo;

    public SourceDocumentReferenceMetaInfo() {
        this.id = null;
        this.imageMetaInfo = null;
        this.audioMetaInfo = null;
        this.videoMetaInfo = null;
        this.textMetaInfo = null;
    }

    public SourceDocumentReferenceMetaInfo(final String sourceDocumentReferenceId, final ImageMetaInfo imageMetaInfo,
                                           final AudioMetaInfo audioMetaInfo, final VideoMetaInfo videoMetaInfo, TextMetaInfo textMetaInfo) {
        this.id = sourceDocumentReferenceId;
        this.imageMetaInfo = imageMetaInfo;
        this.audioMetaInfo = audioMetaInfo;
        this.videoMetaInfo = videoMetaInfo;
        this.textMetaInfo = textMetaInfo;
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

    public TextMetaInfo getTextMetaInfo() {
        return textMetaInfo;
    }
}
