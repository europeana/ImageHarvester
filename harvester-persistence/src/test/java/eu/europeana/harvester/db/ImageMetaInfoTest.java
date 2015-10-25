package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.ImageMetaInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ImageMetaInfoTest {

    @Test
    public void test_color_palette_extraction() {
        final String[] inputPalette = new String[] {"#A9A9A9A9A9A9","#B9B9B9B9BB9","#A9A9A9"};
        final ImageMetaInfo imageMetaInfo = new ImageMetaInfo(null, null,
                null, null, null,
                null, inputPalette, null);
        assertEquals(imageMetaInfo.getColorPalette()[0],"#A9A9A9");
        assertEquals(imageMetaInfo.getColorPalette()[1],"#B9B9B9");
        assertEquals(imageMetaInfo.getColorPalette()[2],"#A9A9A9");
    }

}
