package eu.europeana.harvester.utils;

import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailConfig;
import gr.ntua.image.mediachecker.ImageInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import org.im4java.process.Pipe;
import org.joda.time.DateTime;

import java.io.*;

/**
 * Created by paul on 04/03/15.
 */
public class ThumbnailUtils {

    /**
     * Creates a thumbnail of a downloaded image.
     */
    public final static MediaFile createMediaFileWithThumbnail(final ThumbnailConfig thumbnailConfig,final String source,final String inputUrl,final byte[] inputContent,final String path, final String colorMapPath) throws Exception {
            final ImageInfo originalImageInfo = MediaChecker.getImageInfo(path, colorMapPath);
            Integer expectedThumbnailWidth = null;
            Integer expectedThumbnailHeight = null;

            // Step 1 : compute the width & height of the new thumbnail
            if (thumbnailConfig.getWidth() != 200 && thumbnailConfig.getHeight() != 180) {
                throw new IllegalArgumentException("Cannot generate thumbnails from configuration tasks where width != 200 or height != 180");
            }

            if (thumbnailConfig.getWidth() == 200) {
                // Scenario 1 : The thumbnail generation task must make a thumbnail of width = 200 + proportional height
                if (originalImageInfo.getWidth() < 200) {
                    // Use the original aspect ratio
                    expectedThumbnailWidth = null;
                    expectedThumbnailHeight = null;
                } else {
                    // The width is 200 & the height will be adjusted automatically.
                    expectedThumbnailWidth = 200;
                    expectedThumbnailHeight = null;
                }
            }

            if (thumbnailConfig.getHeight() == 180) {
                // Scenario 2 : The thumbnail generation task must make a thumbnail of height = 180 + proportional width
                if (originalImageInfo.getHeight() < 180) {
                    // Use the original aspect ratio
                    expectedThumbnailWidth = null;
                    expectedThumbnailHeight = null;
                } else {
                    // The height is 180 & the width will be adjusted automatically.
                    expectedThumbnailWidth = null;
                    expectedThumbnailHeight = 180;
                }
            }

            final byte[] newData = createThumbnail(new ByteArrayInputStream(inputContent), expectedThumbnailWidth, expectedThumbnailHeight);

            final String url = inputUrl;
            final String[] temp = url.split("/");
            String name = url;
            if (temp.length > 0) {
                name = temp[temp.length - 1];
            }

            return new MediaFile(source, name, null, null, url,
                    new DateTime(System.currentTimeMillis()), newData, 1, MediaChecker.getMimeType(path), null, thumbnailConfig.getWidth());

    }

    public final static byte[] createThumbnail(final InputStream in, final Integer width, final Integer height) throws Exception {
        final IMOperation op = new IMOperation();
        op.addRawArgs("-colorspace", "RGB");

        if (width != null && height != null) {
            // Scenario 1 : resize both width and height
            op.addRawArgs("-resize", width + "x" + height);
        } else {
            if (width != null & height == null) {
                // Scenario 2 : resize width with height proportional
                op.addRawArgs("-resize", width + "x");
            } else {
                // Scenario 3 : resize height with width proportional
                op.addRawArgs("-resize", "x" + height);
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
