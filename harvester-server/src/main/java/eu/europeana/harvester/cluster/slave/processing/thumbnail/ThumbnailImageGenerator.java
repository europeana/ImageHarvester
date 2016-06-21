package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import org.im4java.core.ConvertCmd;
import org.im4java.core.IMOperation;
import org.im4java.process.Pipe;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * Created by andra on 14.06.2016.
 */
public class ThumbnailImageGenerator extends ThumbnailGenerator {

    public ThumbnailImageGenerator(String colorMathPath) {
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

        // File source path
        op.addImage("-");
        // New file path
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
