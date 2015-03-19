package harvester;

import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.ImageOrientation;
import eu.europeana.harvester.utils.LocalMediaChecker;
import gr.ntua.image.mediachecker.AudioInfo;
import gr.ntua.image.mediachecker.ImageInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import junit.framework.TestCase;
import org.im4java.core.IM4JavaException;
import org.im4java.core.InfoException;
import org.junit.Test;

import java.io.IOException;

public class MediaCheckerTest extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void test() {
        final String path = "./harvester-server/src/test/resources/audio.mid";
        final Integer nrOfIterations = 10000;

        for(int i=0; i<nrOfIterations; i++) {
            try {
                final String type = MediaChecker.getMimeType(path);
                if (type.startsWith("image")) {
                    System.out.println("image");
                    final ImageInfo imageInfo = MediaChecker.getImageInfo(path, "./colormap.png");
                    final Long fileSize = MediaChecker.getFileSize(path);

                    ImageOrientation imageOrientation;
                    if (imageInfo.getWidth() > imageInfo.getHeight()) {
                        imageOrientation = ImageOrientation.LANDSCAPE;
                    } else {
                        imageOrientation = ImageOrientation.PORTRAIT;
                    }

                    ImageMetaInfo imageMetaInfo = new ImageMetaInfo(imageInfo.getWidth(), imageInfo.getHeight(),
                            imageInfo.getMimeType(), imageInfo.getFileFormat(), imageInfo.getColorSpace(),
                            fileSize, imageInfo.getPalette(), imageOrientation);
                    System.out.println(imageInfo.getWidth() + " " + imageInfo.getHeight() + " " + imageInfo.getMimeType()
                            + " " + imageInfo.getFileFormat() + " " + imageInfo.getColorSpace() + " " + fileSize + " " +
                            imageOrientation);
                }
                if (type.startsWith("audio")) {
                    System.out.println("audio");
                    final AudioInfo audioInfo = MediaChecker.getAudioInfo(path);
                    final Long fileSize = MediaChecker.getFileSize(path);

                    System.out.println(audioInfo.getSampleRate() + " " + audioInfo.getBitRate() + " " +
                                    audioInfo.getDuration() + " " + audioInfo.getMimeType() + " " +
                                    audioInfo.getFileFormat() + " " + fileSize + " " + audioInfo.getChannels() + " " +
                                    audioInfo.getBitDepth());
                }
                if (type.startsWith("video")) {
                    System.out.println("video");
                }
                if (type.startsWith("text") || type.startsWith("application/pdf")) {
                    System.out.println("text");

                    final Long fileSize = MediaChecker.getFileSize(path);
                    final Boolean isSearchable = LocalMediaChecker.issearchable(path);

                    Integer getDPI = null;
                    try {
                        getDPI = LocalMediaChecker.getdpi(path);
                    } catch(Exception e) {}

                    System.out.println(fileSize + " " + isSearchable + " " + getDPI);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InfoException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IM4JavaException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
