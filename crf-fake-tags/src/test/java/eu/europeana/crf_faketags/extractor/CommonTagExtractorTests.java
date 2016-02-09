package eu.europeana.crf_faketags.extractor;

import org.junit.Test;

public class CommonTagExtractorTests {

    @Test
    public void canDetectTheMimeTypeType() {
        assert (CommonTagExtractor.isImageMimeType("image/bmp") == true);
        assert (CommonTagExtractor.isImageMimeType("application/yin+xml") == false);

        assert (CommonTagExtractor.isSoundMimeType("audio/vnd.dece.audio") == true);
        assert (CommonTagExtractor.isSoundMimeType("application/yin+xml") == false);

        assert (CommonTagExtractor.isVideoMimeType("video/vnd.fvt") == true);
        assert (CommonTagExtractor.isVideoMimeType("application/yin+xml") == false);

    }

    @Test
    public void canHandleNullMimeTypes() {
        assert (CommonTagExtractor.isSoundMimeType(null) == false);
        assert (CommonTagExtractor.isVideoMimeType(null) == false);
        assert (CommonTagExtractor.isImageMimeType(null) == false);
        assert (CommonTagExtractor.getMimeTypeCode(null) == 0);
    }

}