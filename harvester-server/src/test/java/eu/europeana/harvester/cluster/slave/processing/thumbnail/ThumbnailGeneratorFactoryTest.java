package eu.europeana.harvester.cluster.slave.processing.thumbnail;

import eu.europeana.harvester.cluster.domain.ContentType;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by andra on 21.06.2016.
 */
public class ThumbnailGeneratorFactoryTest {

    @Test
    public void testGetImageThumbnailGenerator() throws Exception {
        ThumbnailGenerator thumbnailImageGenerator = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.IMAGE, "");
        assertTrue(thumbnailImageGenerator instanceof ThumbnailImageGenerator);
    }

    @Test
    public void testGetPDFThumbnailGenerator() throws Exception {
        ThumbnailGenerator thumbnailPDFGenerator = ThumbnailGeneratorFactory.getThumbnailGenerator(ContentType.PDF, "");
        assertTrue(thumbnailPDFGenerator instanceof ThumbnailTextGenerator);
    }
}