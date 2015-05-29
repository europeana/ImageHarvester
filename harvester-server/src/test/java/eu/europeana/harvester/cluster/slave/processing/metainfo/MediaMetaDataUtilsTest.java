package eu.europeana.harvester.cluster.slave.processing.metainfo;

import eu.europeana.harvester.cluster.domain.ContentType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import static eu.europeana.harvester.TestUtils.*;

/**
 * Created by salexandru on 29.05.2015.
 */
public class MediaMetaDataUtilsTest {
    @Test
    public void test_ContentTypeDetection_ImgJpeg() {
        assertEquals(ContentType.IMAGE, MediaMetaDataUtils.classifyUrl(getPath(Image1)));
        assertEquals (ContentType.IMAGE, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "image2.jpeg"));
    }

    @Test
    public void test_ContentTypeDetection_TextPdf() {
        assertEquals (ContentType.TEXT, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "text1.pdf"));
        assertEquals (ContentType.TEXT, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "text2.pdf"));
    }

    @Test
    public void test_ContentTypeDetection_AudioMp3() {
        assertEquals (ContentType.AUDIO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "audio1.mp3"));
        assertEquals (ContentType.AUDIO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "audio2.mp3"));
    }

    @Test
    public void test_ContentTypeDetection_VideoMpg() {
        assertEquals (ContentType.VIDEO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "video1.mpg"));
        assertEquals (ContentType.VIDEO, MediaMetaDataUtils.classifyUrl(PATH_PREFIX + "video2.mpg"));
    }

    @Test
    public void test_ContentTypeDetection_Unknown() {
        assertEquals(ContentType.UNKNOWN, MediaMetaDataUtils.classifyUrl("http://www.google.com"));
    }
}
