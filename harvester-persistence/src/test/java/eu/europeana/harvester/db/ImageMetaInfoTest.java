package eu.europeana.harvester.db;

import eu.europeana.harvester.domain.ImageMetaInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ImageMetaInfoTest {

    @Test
    public void test_color_palette_extraction() {
        final String[] inputPalette = new String[] {"#A9A9A9A9A9A9","#B9B9B9B9BB9","#A9A9A9"};
        final String[] trimmedColorPalette = ImageMetaInfo.trimColorPalette(inputPalette);
        assertEquals(trimmedColorPalette[0],"#A9A9A9");
        assertEquals(trimmedColorPalette[1],"#B9B9B9");
        assertEquals(trimmedColorPalette[2],"#A9A9A9");
    }

}
