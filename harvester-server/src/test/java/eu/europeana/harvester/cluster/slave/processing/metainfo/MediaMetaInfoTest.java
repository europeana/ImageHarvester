package eu.europeana.harvester.cluster.slave.processing.metainfo;

import eu.europeana.harvester.domain.AudioMetaInfo;
import eu.europeana.harvester.domain.ImageMetaInfo;
import eu.europeana.harvester.domain.TextMetaInfo;
import eu.europeana.harvester.domain.VideoMetaInfo;
import gr.ntua.image.mediachecker.MediaChecker;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import static eu.europeana.harvester.TestUtils.*;

/**
 * Created by salexandru on 29.05.2015.
 */
public class MediaMetaInfoTest {
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Test
    public void test_MetadataExtraction_Img1() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Image1));
        final ImageMetaInfo metaInfo = metaInfoTuple.getImageMetaInfo();

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull("audio meta info should be null, when processing an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when processing an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when  processing an image", metaInfoTuple.getTextMetaInfo());

        assertTrue(IMAGE_MIMETYPE.equalsIgnoreCase(metaInfo.getMimeType()));
        assertEquals((Long) 1399538L, metaInfo.getFileSize());
        assertEquals((Integer) 2500, metaInfo.getWidth());
        assertEquals((Integer) 1737, metaInfo.getHeight());
        assertArrayEquals(MediaChecker.getImageInfo(getPath(Image1), PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
        assertTrue("sRGB".equalsIgnoreCase(metaInfo.getColorSpace()));
        assertTrue(IMAGE_FORMAT.equalsIgnoreCase(metaInfo.getFileFormat()));
    }

    @Test
    public void test_MetadataExtraction_Img2() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Image2));
        final ImageMetaInfo metaInfo = metaInfoTuple.getImageMetaInfo();

        assertNotNull("Image meta info must not be null!", metaInfo);
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());

        assertTrue(IMAGE_MIMETYPE.equalsIgnoreCase(metaInfo.getMimeType()));
        assertEquals((Long) 1249616L, metaInfo.getFileSize());
        assertEquals((Integer) 2500, metaInfo.getWidth());
        assertEquals((Integer) 1702, metaInfo.getHeight());
        assertTrue("sRGB".equalsIgnoreCase(metaInfo.getColorSpace()));
        assertTrue(IMAGE_FORMAT.equalsIgnoreCase(metaInfo.getFileFormat()));
        assertArrayEquals(MediaChecker.getImageInfo(getPath(Image2), PATH_COLORMAP).getPalette(),
                          metaInfo.getColorPalette());
    }

    @Test
    public void test_MetaDataExtraction_Audio1() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Audio1));
        final AudioMetaInfo metaInfo = metaInfoTuple.getAudioMetaInfo();

        assertNotNull("Audio meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());

        assertTrue(AUDIO_MIMETYPE.equalsIgnoreCase(metaInfo.getMimeType()));
        assertEquals((Long) 1388197L, metaInfo.getFileSize());
        assertEquals((Long) 198313L, metaInfo.getDuration());
        assertEquals((Integer)56000, metaInfo.getBitRate());
        assertEquals((Integer)22050, metaInfo.getSampleRate());
        assertEquals((Integer) 1, metaInfo.getChannels());
        assertEquals((Integer) 16, metaInfo.getBitDepth());
        assertTrue(AUDIO_FORMAT.equalsIgnoreCase(metaInfo.getFileFormat()));
    }

    @Test
    public void test_MetaDataExtraction_Audio2() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Audio2));
        final AudioMetaInfo metaInfo = metaInfoTuple.getAudioMetaInfo();

        assertNotNull("Audio meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals((Long) 1234779L, metaInfo.getFileSize());
        assertEquals((Long) 176397L,  metaInfo.getDuration());
        assertEquals((Integer)56000, metaInfo.getBitRate());
        assertEquals((Integer)22050, metaInfo.getSampleRate());
        assertEquals((Integer) 1, metaInfo.getChannels());
        assertEquals((Integer) 16, metaInfo.getBitDepth());
        assertTrue(AUDIO_FORMAT.equalsIgnoreCase(metaInfo.getFileFormat()));
        assertTrue(AUDIO_MIMETYPE.equalsIgnoreCase(metaInfo.getMimeType()));
    }

    @Test
    public void test_MetaDataExtraction_Video1() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Video1));
        final VideoMetaInfo metaInfo = metaInfoTuple.getVideoMetaInfo();

        assertNotNull("Video meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals((Long) 7662202L, metaInfo.getFileSize());
        assertEquals((Integer) 1150000, metaInfo.getBitRate());
        assertEquals((Integer) 288, metaInfo.getHeight());
        assertEquals((Integer) 352, metaInfo.getWidth());
        assertTrue(VIDEO_MIMETYPE.equalsIgnoreCase(metaInfo.getMimeType()));
        assertEquals ("352x288", metaInfo.getResolution());
        assertEquals((Double) 25.0, metaInfo.getFrameRate());
        assertEquals("mpeg1video", metaInfo.getCodec());
        assertEquals((Long) 44745L, metaInfo.getDuration());
    }

    @Test
    public void test_MetaDataExtraction_Video2() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Video2));
        final VideoMetaInfo metaInfo = metaInfoTuple.getVideoMetaInfo();

        assertNotNull("Video meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("text meta info should be null, when metaInfoTuple an image", metaInfoTuple.getTextMetaInfo());
        assertEquals((Long) 9151124L, metaInfo.getFileSize());
        assertEquals((Integer) 1152000, metaInfo.getBitRate());
        assertEquals((Integer) 288, metaInfo.getHeight());
        assertEquals((Integer) 352, metaInfo.getWidth());
        assertEquals ("352x288", metaInfo.getResolution());
        assertEquals ((Double)25.0, metaInfo.getFrameRate());
        assertEquals("mpeg1video", metaInfo.getCodec());
        assertEquals((Long) 53628L, metaInfo.getDuration());
        assertTrue(VIDEO_MIMETYPE.equalsIgnoreCase(metaInfo.getMimeType()));
    }

    @Test
    public void test_MetaDataExtraction_Text1() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Text1));
        final TextMetaInfo metaInfo = metaInfoTuple.getTextMetaInfo();

        assertNotNull("Text meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertNotNull(metaInfo);
        assertEquals((Long)7904453L, metaInfo.getFileSize());
        assertTrue(metaInfo.getIsSearchable());
        assertEquals((Integer) (-1), metaInfo.getResolution());
    }

    @Test
    public void test_MetaDataExtraction_Text2() throws Exception {
        final MediaMetaInfoTuple metaInfoTuple = new MediaMetaInfoExtractor(PATH_COLORMAP).extract(getPath(Text2));
        final TextMetaInfo metaInfo = metaInfoTuple.getTextMetaInfo();

        assertNotNull("Text meta info must not be null!", metaInfo);
        assertNull("image meta info should be null, when metaInfoTuple an image", metaInfoTuple.getImageMetaInfo());
        assertNull("audio meta info should be null, when metaInfoTuple an image", metaInfoTuple.getAudioMetaInfo());
        assertNull("video meta info should be null, when metaInfoTuple an image", metaInfoTuple.getVideoMetaInfo());
        assertEquals((Long)13566851L, metaInfo.getFileSize());
        assertFalse(metaInfo.getIsSearchable());
        assertEquals ( (Integer)(-1), metaInfo.getResolution());
    }
}
