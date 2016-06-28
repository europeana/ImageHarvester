package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import org.im4java.process.Pipe;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by andra on 14.06.2016.
 */
public class ThumbnailTextGenerator extends ThumbnailGenerator {

    private static final String PDF_PAGE_NO_TO_SHOW = "[0]";

    public ThumbnailTextGenerator(String colorMathPath) {
        super(colorMathPath);
    }

    protected byte[] createThumbnail(final InputStream in, final Integer width, final Integer height, final byte[] originalFileInfo) throws Exception {
        final IMOperation op = new IMOperation();

        if (width != null && height != null) {
            // Scenario 1 : resize both width and height
            op.addRawArgs("-thumbnail", width + "x" + height);
        } else {
            if (width != null && height == null) {
                // Scenario 2 : resize width with height proportional
                op.addRawArgs("-thumbnail", width + "x");
            } else if (height != null && width == null) {
                // Scenario 3 : resize height with width proportional
                op.addRawArgs("-thumbnail", "x" + height);
            } else {
                // Scenario 4 : use original values for both width and height
                op.addRawArgs("-thumbnail", "xx");
            }
        }

        // Set white background
        op.background("white");
        // Apply alpha remove
        op.alpha("remove");
        // Source file path, PDF_PAGE_NO_TO_SHOW applies conversion only on the first page
        op.addImage("-" + PDF_PAGE_NO_TO_SHOW);
        // New file path, generating a jpg format thumbnail
        op.addImage(IMAGE_OUTPUT_FORMAT + "-");

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