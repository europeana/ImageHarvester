package eu.europeana.harvester.cluster.master;

import eu.europeana.harvester.domain.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by paul on 07/08/15.
 */
public class SourceDocumentReferenceMetaInfoTests {

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Test
    public void canAvoidMergingNullExistingDoc() {
        thrown.expect(IllegalArgumentException.class);

        final SourceDocumentReferenceMetaInfo docWithOnlyColorPalette = new SourceDocumentReferenceMetaInfo("", new ImageMetaInfo(null, null,
                null, null, null,
                null, new String[]{"1", "2", "3"}, null),
                null, null, null);

        SourceDocumentReferenceMetaInfo.mergeColorPalette(null,docWithOnlyColorPalette);
    }

    @Test
    public void canAvoidMergingNullNewDoc() {
        thrown.expect(IllegalArgumentException.class);

        final SourceDocumentReferenceMetaInfo docWithOnlyColorPalette = new SourceDocumentReferenceMetaInfo("", new ImageMetaInfo(null, null,
                null, null, null,
                null, new String[]{"1", "2", "3"}, null),
                null, null, null);

        SourceDocumentReferenceMetaInfo.mergeColorPalette(null,docWithOnlyColorPalette);
    }

    @Test
    public void canMergeColorPaletteWithNonNullDocs() {

        final SourceDocumentReferenceMetaInfo docWithOnlyColorPalette = new SourceDocumentReferenceMetaInfo("", new ImageMetaInfo(null, null,
                null, null, null,
                null, new String[]{"1", "2", "3"}, null),
                null, null, null);

        final SourceDocumentReferenceMetaInfo docWithMoreThanColorPalette = new SourceDocumentReferenceMetaInfo("", new ImageMetaInfo(100, 200,
                null, null, null,
                null, new String[]{"1"}, null),
                new AudioMetaInfo(), new VideoMetaInfo(), new TextMetaInfo());

        // Merging operates only on color palette
        assertEquals(new Integer(200), SourceDocumentReferenceMetaInfo.mergeColorPalette(
                docWithMoreThanColorPalette, docWithOnlyColorPalette).getImageMetaInfo().getHeight());
        assertEquals(new Integer(100), SourceDocumentReferenceMetaInfo.mergeColorPalette(
                docWithMoreThanColorPalette, docWithOnlyColorPalette).getImageMetaInfo().getWidth());
        assertEquals(3, SourceDocumentReferenceMetaInfo.mergeColorPalette(
                docWithMoreThanColorPalette, docWithOnlyColorPalette).getImageMetaInfo().getColorPalette().length);
        assertNotNull(SourceDocumentReferenceMetaInfo.mergeColorPalette(
                docWithMoreThanColorPalette, docWithOnlyColorPalette).getAudioMetaInfo());
        assertNotNull(SourceDocumentReferenceMetaInfo.mergeColorPalette(
                docWithMoreThanColorPalette, docWithOnlyColorPalette).getVideoMetaInfo());
        assertNotNull(SourceDocumentReferenceMetaInfo.mergeColorPalette(
                docWithMoreThanColorPalette, docWithOnlyColorPalette).getTextMetaInfo());

    }
}
