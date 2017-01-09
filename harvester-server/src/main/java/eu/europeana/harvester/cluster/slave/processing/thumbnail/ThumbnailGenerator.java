package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaDataUtils;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailType;
import gr.ntua.image.mediachecker.ImageInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import org.im4java.process.Pipe;
import org.joda.time.DateTime;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class ThumbnailGenerator {

    private final String colorMapPath;

    public ThumbnailGenerator(String colorMapPath) {
        this.colorMapPath = colorMapPath;
    }

    /**
     * Creates a thumbnail of a downloaded image.
     */
    public MediaFile createMediaFileWithThumbnail(final Integer expectedHeight,final Integer expectedWidth,final String currentProcessId,final String originalImageUrl,final byte[] originalImageContent,final String originalImagePath) throws Exception {
        if (ContentType.IMAGE != MediaMetaDataUtils.classifyUrl(originalImagePath)) {
            return null;
        }

        final ImageInfo originalImageInfo = MediaChecker.getImageInfo(originalImagePath, colorMapPath);
        Integer thumbnailResizedToWidth = null;
        Integer thumbnailResizedToHeight = null;


        // Step 1 : compute the width & height of the new thumbnail
        if (expectedWidth != ThumbnailType.MEDIUM.getWidth() && expectedWidth != ThumbnailType.LARGE.getWidth()) {
            throw new IllegalArgumentException("Cannot generate thumbnails from configuration tasks where width != "+ThumbnailType.MEDIUM.getHeight() + " or width != "+ThumbnailType.LARGE.getHeight());
        }

        if (expectedWidth == ThumbnailType.MEDIUM.getWidth()) {
            // Scenario 2 : The thumbnail generation task must make a thumbnail of width = 200 + proportional height
            if (originalImageInfo.getWidth() < ThumbnailType.MEDIUM.getWidth()) {
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
            if (originalImageInfo.getWidth() < ThumbnailType.LARGE.getWidth()) {
                // Use the original aspect ratio
                thumbnailResizedToWidth = null;
                thumbnailResizedToHeight = null;
            } else {
                // The width is 200 & the height will be adjusted automatically.
                thumbnailResizedToWidth = ThumbnailType.LARGE.getWidth();
                thumbnailResizedToHeight = null;
            }
        }


        final byte[] newData = createThumbnail(new ByteArrayInputStream(originalImageContent), thumbnailResizedToWidth, thumbnailResizedToHeight);

        final String url = originalImageUrl;
        final String[] temp = url.split("/");
        String name = url;
        if (temp.length > 0) {
            name = temp[temp.length - 1];
        }

        final ThumbnailType expectedThumbnailType = thumbnailTypeFromExpectedSize(expectedHeight,expectedWidth);

        if (expectedThumbnailType == null) throw new IllegalArgumentException("The expected thumbnail height "+expectedHeight+" or width "+expectedWidth+" do not match any of the hardcoded presets");

        return new MediaFile(currentProcessId, name, null, null, url,
                new DateTime(System.currentTimeMillis()), newData, 1, MediaChecker.getMimeType(originalImagePath), null, newData.length)
                .withId(MediaFile.generateIdFromUrlAndSizeType(originalImageUrl,expectedThumbnailType.name()));

    }

    private final static ThumbnailType thumbnailTypeFromExpectedSize(final Integer expectedHeight,final Integer expectedWidth) {
        for (final ThumbnailType type: ThumbnailType.values()) {
            if (type.getHeight() == expectedHeight && type.getWidth() == expectedWidth) {
                return type;
            }
        }

        return null;
    }

    private final static byte[] createThumbnail(final InputStream in, final Integer width, final Integer height) throws Exception {
        final IMOperation op = new IMOperation();

        if (width != null && height != null) {
            // Scenario 1 : resize both width and height
            op.addRawArgs("-thumbnail", width + "x" + height);
        } else {
            if (width != null & height == null) {
                // Scenario 2 : resize width with height proportional
                op.addRawArgs("-thumbnail", width + "x");
            } else {
                // Scenario 3 : resize height with width proportional
                op.addRawArgs("-thumbnail", "x" + height);
            }
        }

        op.addImage("-");
        op.addImage("-");

        final Pipe pipeIn = new Pipe(in, null);
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final Pipe pipeOut = new Pipe(null, out);

        final ConvertCmd convert = new ConvertCmd();
        convert.setInputProvider(pipeIn);
        convert.setOutputConsumer(pipeOut);
        convert.run(op);

        return out.toByteArray();
    }

}
