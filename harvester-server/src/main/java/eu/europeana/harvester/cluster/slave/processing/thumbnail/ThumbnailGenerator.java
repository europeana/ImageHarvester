package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailType;
import gr.ntua.image.mediachecker.ImageInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public abstract class ThumbnailGenerator {

    private String colorMapPath;

    public ThumbnailGenerator(String colorMapPath) {
        this.colorMapPath = colorMapPath;
    }

    public String getColorMapPath() {
        return colorMapPath;
    }

    /**
     * Creates a thumbnail of a downloaded media file (image or PDF)
     */
    public MediaFile createMediaFileWithThumbnail (final Integer expectedWidth, final Integer expectedHeight, final String currentProcessId, final String originalFileUrl, final byte[] originalFileContent, final String originalFilePath) throws Exception {
        final ImageInfo originalFileInfo = MediaChecker.getImageInfo(originalFilePath, getColorMapPath());
        Integer thumbnailResizedToWidth = null;
        Integer thumbnailResizedToHeight = null;

        // Step 1 : compute the width & height of the new thumbnail
        if (expectedWidth != ThumbnailType.MEDIUM.getWidth() && expectedWidth != ThumbnailType.LARGE.getWidth()) {
            throw new IllegalArgumentException("Cannot generate thumbnails from configuration tasks where width != "+ThumbnailType.MEDIUM.getHeight() + " or width != "+ThumbnailType.LARGE.getHeight());
        }

        if (expectedWidth == ThumbnailType.MEDIUM.getWidth()) {
            // Scenario 2 : The thumbnail generation task must make a thumbnail of width = 200 + proportional height
            if (originalFileInfo.getWidth() < ThumbnailType.MEDIUM.getWidth()) {
                // Use the original aspect ratio
                thumbnailResizedToWidth = null;
                thumbnailResizedToHeight = null;
            } else {
                // The width is 200 & the height will be adjusted automatically.
                thumbnailResizedToWidth = ThumbnailType.MEDIUM.getWidth();
                thumbnailResizedToHeight = null;
            }
        }

        if (expectedWidth == ThumbnailType.LARGE.getWidth()) {
            // Scenario 3 : The thumbnail generation task must make a thumbnail of width = 400 + proportional height
            if (originalFileInfo.getWidth() < ThumbnailType.LARGE.getWidth()) {
                // Use the original aspect ratio
                thumbnailResizedToWidth = null;
                thumbnailResizedToHeight = null;
            } else {
                // The width is 200 & the height will be adjusted automatically.
                thumbnailResizedToWidth = ThumbnailType.LARGE.getWidth();
                thumbnailResizedToHeight = null;
            }
        }

        final String url = originalFileUrl;
        final String[] temp = url.split("/");
        String name = url;
        if (temp.length > 0) {
            name = temp[temp.length - 1];
        }

        final byte[] newData = createThumbnail(new ByteArrayInputStream(originalFileContent), thumbnailResizedToWidth, thumbnailResizedToHeight, originalFileContent);

        final ThumbnailType expectedThumbnailType = thumbnailTypeFromExpectedSize(expectedHeight, expectedWidth);

        if (expectedThumbnailType == null) throw new IllegalArgumentException("The expected thumbnail height "+expectedHeight+" or width "+expectedWidth+" do not match any of the hardcoded presets");

        return new MediaFile(currentProcessId, name, null, null, url,
                new DateTime(System.currentTimeMillis()), newData, 1, MediaChecker.getMimeType(originalFilePath), null, expectedWidth)
                .withId(MediaFile.generateIdFromUrlAndSizeType(originalFileUrl, expectedThumbnailType.name()));
    }

    private final ThumbnailType thumbnailTypeFromExpectedSize(final Integer expectedHeight, final Integer expectedWidth) {
        for (final ThumbnailType type: ThumbnailType.values()) {
            if (type.getHeight() == expectedHeight && type.getWidth() == expectedWidth) {
                return type;
            }
        }

        return null;
    }

    /**
     * Manages im4java thumbnail converting call
     */
    protected abstract byte[] createThumbnail(final InputStream in, final Integer width, final Integer height, final byte[] originalFileInfo) throws Exception;
}