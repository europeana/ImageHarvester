package eu.europeana.harvester.domain;

import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The public language
 * The domain model exposed by the media client through it's public interface.
 */
public class MediaFile {

    public static final String generateIdFromUrlAndSizeType(final String url, final String sizeType) throws
                                                                                                     NoSuchAlgorithmException {
        return new StringBuilder().append(DigestUtils.md5Hex(url)).append("-").append(sizeType).toString();
    }

    public static final MediaFile createMinimalMediaFileWithSizeType(String sizeType,
                                                                     String source,
                                                                     String name,
                                                                     String originalUrl,
                                                                     DateTime createdAt,
                                                                     byte[] content,
                                                                     String contentType,
                                                                     Integer size) throws NoSuchAlgorithmException {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");

        return new MediaFile(MediaFile.generateIdFromUrlAndSizeType(originalUrl, sizeType),
                             source,
                             name,
                             Collections.<String>emptyList(),
                             new String(messageDigest.digest(content)),
                             originalUrl,
                             createdAt,
                             content,
                             0,
                             contentType,
                             Collections.<String, String>emptyMap(),
                             size);
    }

    /**
     * The md5 of the original url
     */
    private final String id;

    /**
     * The process that created it.
     */
    private final String source;

    /**
     * Length of the byte array, which is the actual content of a file.
     */
    private final Long length;

    /**
     * File name.
     */
    private final String name;

    /**
     * Aliases from the metadata.
     */
    private final List<String> aliases;

    /**
     * The observed MD5 during transfer.
     */
    private final String contentMd5;

    /**
     * The source url of the file.
     */
    private final String originalUrl;

    /**
     * Time of saving the file to the DB.
     */
    private final DateTime createdAt;

    /**
     * The content of the file in a byte array format.
     */
    private final byte[] content;

    /**
     * The versionNumber of the file.
     */
    private final Integer versionNumber;

    /**
     * The content type.
     */
    private final String contentType;

    /**
     * The thumbnails metadata: height, width, ...
     */
    private final Map<String, String> metaData;

    /**
     * The size of the thumbnail
     */
    private final Integer size;

    public MediaFile(String source,
                     String name,
                     List<String> aliases,
                     String contentMd5,
                     String originalUrl,
                     DateTime createdAt,
                     byte[] content,
                     Integer versionNumber,
                     String contentType,
                     Map<String, String> metaData,
                     Integer size) throws NoSuchAlgorithmException {

        this.id = MediaFile.generateIdFromUrlAndSizeType(originalUrl, "ORIGINAL");
        this.source = source;
        this.length = null == content ? 0L : (long) content.length;
        this.name = name;
        this.aliases = aliases;
        this.contentMd5 = contentMd5;
        this.originalUrl = originalUrl;
        this.createdAt = createdAt;
        this.content = content;
        this.versionNumber = versionNumber;
        this.contentType = contentType;
        this.metaData = metaData;
        this.size = size;
    }

    public MediaFile(String id,
                     String source,
                     String name,
                     List<String> aliases,
                     String contentMd5,
                     String originalUrl,
                     DateTime createdAt,
                     byte[] content,
                     Integer versionNumber,
                     String contentType,
                     Map<String, String> metaData,
                     Integer size) {
        this.id = id;
        this.source = source;
        this.length = null == content ? 0L : (long) content.length;
        this.name = name;
        this.aliases = aliases;
        this.contentMd5 = contentMd5;
        this.originalUrl = originalUrl;
        this.createdAt = createdAt;
        this.content = content;
        this.versionNumber = versionNumber;
        this.contentType = contentType;
        this.metaData = metaData;
        this.size = size;
    }

    public String getId() {
        return id;
    }

    public String getSource() {
        return source;
    }

    public Long getLength() {
        return length;
    }

    public String getName() {
        return name;
    }

    public List<String> getAliases() {
        return aliases;
    }

    public String getContentMd5() {
        return contentMd5;
    }

    public String getOriginalUrl() {
        return originalUrl;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public byte[] getContent() {
        return content;
    }

    public Integer getVersionNumber() {
        return versionNumber;
    }

    public String getContentType() {
        return contentType;
    }

    public Map<String, String> getMetaData() {
        return metaData;
    }

    public Integer getSize() {
        return size;
    }

    public MediaFile withColorPalette(String[] colorPalette) {
        final Map<String, String> newMetaData = new HashMap<>();
        int                       i           = 0;
        for (String color : colorPalette) {
            newMetaData.put("color" + i, color);
            i++;
        }
        newMetaData.put("size", String.valueOf(this.size));

        return new MediaFile(this.id,
                             this.source,
                             this.name,
                             this.aliases,
                             this.contentMd5,
                             this.originalUrl,
                             this.createdAt,
                             this.content,
                             this.versionNumber,
                             this.contentType,
                             newMetaData,
                             this.size);
    }

    public MediaFile withId(final String id) {
        return new MediaFile(id,
                             this.source,
                             this.name,
                             this.aliases,
                             this.contentMd5,
                             this.originalUrl,
                             this.createdAt,
                             this.content,
                             this.versionNumber,
                             this.contentType,
                             this.metaData,
                             this.size);

    }

}
