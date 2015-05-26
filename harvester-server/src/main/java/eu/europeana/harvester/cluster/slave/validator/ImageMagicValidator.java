package eu.europeana.harvester.cluster.slave.validator;

import com.google.common.io.ByteStreams;

import java.io.IOException;

public class ImageMagicValidator {
    final String expectedImageMagicVersion;

    public ImageMagicValidator(final String expectedImageMagicVersion) {
        this.expectedImageMagicVersion = expectedImageMagicVersion;
    }

    public void doNothingOrThrowException() throws Exception {
        try {
            final String output = new String(ByteStreams.toByteArray(Runtime.getRuntime().exec("convert --version").getInputStream()));
            if (!output.contains(expectedImageMagicVersion)) {
                throw new Exception("ImageMagic version is installed on the system but has the wrong version. Expected version " + expectedImageMagicVersion + ". Current version : " + output);
            }
        } catch (IOException e) {
            throw new Exception("ImageMagic is not installed in the system. Please install expected version : " + expectedImageMagicVersion+" or higher.");
        }

    }

}
