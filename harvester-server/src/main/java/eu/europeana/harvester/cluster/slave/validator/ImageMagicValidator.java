package eu.europeana.harvester.cluster.slave.validator;

import com.google.common.io.ByteStreams;
import org.apache.maven.shared.utils.StringUtils;

import java.io.IOException;
import java.util.List;

public class ImageMagicValidator {
    final List<String> expectedImageMagicVersions;

    public ImageMagicValidator(final List<String> expectedImageMagicVersions) {
        this.expectedImageMagicVersions = expectedImageMagicVersions;
    }

    public void doNothingOrThrowException() throws Exception {
        try {
            final String output = new String(ByteStreams.toByteArray(Runtime.getRuntime().exec("convert --version").getInputStream()));
            boolean valid = false;
            for (final String expectedImageMagicVersion : expectedImageMagicVersions)
            if (output.contains(expectedImageMagicVersion)) {
                valid = true;
                break;
            }
            if (!valid) throw new Exception("ImageMagic version is installed on the system but has the wrong version. Expected one of version(s) " + StringUtils.concatenate(expectedImageMagicVersions) + ". Current version : " + output);
        } catch (IOException e) {
            throw new Exception("ImageMagic is not installed in the system. Please install one of expected version(s) : " + StringUtils.concatenate(expectedImageMagicVersions)+" or higher.");
        }

    }

}
