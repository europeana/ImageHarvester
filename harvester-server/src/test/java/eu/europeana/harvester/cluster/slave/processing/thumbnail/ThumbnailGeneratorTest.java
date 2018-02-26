package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import eu.europeana.harvester.cluster.domain.ContentType;
import eu.europeana.harvester.domain.MediaFile;
import eu.europeana.harvester.domain.ThumbnailType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static eu.europeana.harvester.TestUtils.*;
import static org.junit.Assert.*;

/**
 * Created by salexandru on 29.05.2015.
 */
public class ThumbnailGeneratorTest {

    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    private boolean almostSameSize(final byte[] first, final byte[] second) {
        return (((first.length / second.length) > 0.95) || ((second.length / first.length) > 0.95));
    }

    @Test
    public void test_ThumbnailGeneration_Image1_Medium() throws Exception {
        final Integer width  = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(Image1),
                                                                                           filesInBytes.get(Image1),
                                                                                           getPath(Image1));
        assertTrue(IMAGE_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        // (oh, I say.)
//        assertEquals(width, thumbnail.getSize());
        assertEquals(getPath(Image1), thumbnail.getOriginalUrl());
        assertEquals(Image1, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(Image1ThumbnailMedium), thumbnail.getContent()));
    }

    @Test
    public void test_ThumbnailGeneration_Image1_Large() throws Exception {
        final Integer width  = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(Image1),
                                                                                           filesInBytes.get(Image1),
                                                                                           getPath(Image1));
        assertTrue(IMAGE_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        // (oh, I say.)
//        assertEquals(width, thumbnail.getSize());
        assertEquals(getPath(Image1), thumbnail.getOriginalUrl());
        assertEquals(Image1, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(Image1ThumbnailLarge), thumbnail.getContent()));
    }

    @Test
    public void test_ThumbnailGeneration_Image2_Medium() throws Exception {
        final Integer width  = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();
        System.out.println(getPath(Image2));
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(Image2),
                                                                                           filesInBytes.get(Image2),
                                                                                           getPath(Image2));
        assertTrue(IMAGE_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        // (oh, I say.)
//        assertEquals(width, thumbnail.getSize());
        assertEquals(getPath(Image2), thumbnail.getOriginalUrl());
        assertEquals(Image2, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(Image2ThumbnailMedium), thumbnail.getContent()));
    }

    @Test
    public void test_ThumbnailGeneration_Image2_Large() throws Exception {
        final Integer width  = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(Image2),
                                                                                           filesInBytes.get(Image2),
                                                                                           getPath(Image2));
        assertTrue(IMAGE_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        // (oh, I say.)
//        assertEquals(width, thumbnail.getSize());
        assertEquals(getPath(Image2), thumbnail.getOriginalUrl());
        assertEquals(Image2, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(Image2ThumbnailLarge), thumbnail.getContent()));
    }

    @Test(expected = Exception.class)
    public void test_ThumbnailGeneration_Fail_Audio() throws Exception {
        final Integer width  = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.AUDIO, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(height,
                                                                                           width,
                                                                                           "",
                                                                                           getPath(Audio2),
                                                                                           filesInBytes.get(Audio2),
                                                                                           getPath(Audio2));
        assertNull(thumbnail);
    }

    @Test(expected = Exception.class)
    public void test_ThumbnailGeneration_Fail_Video() throws Exception {
        final Integer width  = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.VIDEO, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(height,
                                                                                           width,
                                                                                           "",
                                                                                           getPath(Video2),
                                                                                           filesInBytes.get(Video2),
                                                                                           getPath(Video2));
        assertNull(thumbnail);
    }

    @Test(expected = Exception.class)
    public void test_ThumbnailGeneration_Fail_TextPlain() throws Exception {
        final Integer width  = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.NON_PDF_TEXT,
                                                                                    PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(height,
                                                                                           width,
                                                                                           "",
                                                                                           getPath(Text1),
                                                                                           filesInBytes.get(Text1),
                                                                                           getPath(Text1));
        assertNull(thumbnail);
    }

    @Test
    public void test_ThumbnailGeneration_PDF1_Medium() throws Exception {
        final Integer width  = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.PDF, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(PDF1),
                                                                                           filesInBytes.get(PDF1),
                                                                                           getPath(PDF1));
        assertTrue(PDF_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        assertEquals(getPath(PDF1), thumbnail.getOriginalUrl());
        assertEquals(PDF1, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(PDF1), thumbnail.getContent()));
    }

    @Test
    public void test_ThumbnailGeneration_PDF2_Medium() throws Exception {
        final Integer width  = ThumbnailType.MEDIUM.getWidth();
        final Integer height = ThumbnailType.MEDIUM.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.PDF, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(PDF2),
                                                                                           filesInBytes.get(PDF2),
                                                                                           getPath(PDF2));
        assertTrue(PDF_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        assertEquals(getPath(PDF2), thumbnail.getOriginalUrl());
        assertEquals(PDF2, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(PDF2), thumbnail.getContent()));
    }

    @Test
    public void test_ThumbnailGeneration_PDF3_Large() throws Exception {
        final Integer width  = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.PDF, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(PDF3),
                                                                                           filesInBytes.get(PDF3),
                                                                                           getPath(PDF3));
        assertTrue(PDF_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        assertEquals(getPath(PDF3), thumbnail.getOriginalUrl());
        assertEquals(PDF3, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(PDF3), thumbnail.getContent()));
    }

    @Test
    public void test_ThumbnailGeneration_PDF4_Large() throws Exception {
        final Integer width  = ThumbnailType.LARGE.getWidth();
        final Integer height = ThumbnailType.LARGE.getHeight();
        final MediaFile thumbnail = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.PDF, PATH_COLORMAP)
                                                             .createMediaFileWithThumbnail(width,
                                                                                           height,
                                                                                           "",
                                                                                           getPath(PDF4),
                                                                                           filesInBytes.get(PDF4),
                                                                                           getPath(PDF4));
        assertTrue(PDF_MIMETYPE.equalsIgnoreCase(thumbnail.getContentType()));
        assertEquals("", thumbnail.getSource());
        assertEquals(getPath(PDF4), thumbnail.getOriginalUrl());
        assertEquals(PDF4, thumbnail.getName());
        assertTrue(almostSameSize(filesInBytes.get(PDF4), thumbnail.getContent()));
    }
}