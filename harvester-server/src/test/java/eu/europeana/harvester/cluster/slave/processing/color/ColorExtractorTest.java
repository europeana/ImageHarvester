package eu.europeana.harvester.cluster.slave.processing.color;

import eu.europeana.harvester.domain.ImageMetaInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static eu.europeana.harvester.TestUtils.*;

/**
 * Created by salexandru on 29.05.2015.
 */
public class ColorExtractorTest {
    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }

        @Override
        protected  void finished(Description description) {
            System.out.println("Stopping test: " + description.getMethodName());
        }
    };
    @Test
    public void test_ColorPaletteExtraction_Image1() throws Exception {
        final ImageMetaInfo metaInfo = new ColorExtractor(PATH_COLORMAP).colorExtraction(getPath(Image1));

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull(metaInfo.getMimeType());
        assertNull(metaInfo.getFileSize());
        assertNull(metaInfo.getWidth());
        assertNull(metaInfo.getHeight());
        assertArrayEquals(MediaChecker.getImageInfo(getPath(Image1), PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
        assertNull(metaInfo.getColorSpace());
        assertNull(metaInfo.getFileFormat());
    }

    @Test
    public void test_ColorPaletteExtraction_Image2() throws Exception {
        final ImageMetaInfo metaInfo = new ColorExtractor(PATH_COLORMAP).colorExtraction(getPath(Image2));

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull(metaInfo.getMimeType());
        assertNull(metaInfo.getFileSize());
        assertNull(metaInfo.getWidth());
        assertNull(metaInfo.getHeight());
        assertArrayEquals(MediaChecker.getImageInfo(getPath(Image2), PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
        assertNull(metaInfo.getColorSpace());
        assertNull(metaInfo.getFileFormat());
    }

    @Test
    public void test_ColorPaletteExtraction_FailForVideo() throws Exception {
        final ImageMetaInfo metaInfo = new ColorExtractor(PATH_COLORMAP).colorExtraction(getPath(Video1));

        assertNull(metaInfo);
    }

    @Test
    public void test_ColorPaletteExtraction_FailForText() throws Exception {
        final ImageMetaInfo metaInfo = new ColorExtractor(PATH_COLORMAP).colorExtraction(getPath(Text1));

        assertNull(metaInfo);
    }
}
