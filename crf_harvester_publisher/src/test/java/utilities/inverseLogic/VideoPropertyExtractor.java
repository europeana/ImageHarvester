package utilities.inverseLogic;

import eu.europeana.publisher.logic.extractor.TagEncoding;

public class VideoPropertyExtractor {
    public static String getQuality(Integer tag) {
        final Integer qualityCode = TagEncoding.VIDEO_QUALITY.extractValue(tag);

        if (1 == qualityCode) {
            return "true";
        }

        return "";
    }

    public static Long getDuration(Integer tag) {
        final Integer durationCode = TagEncoding.VIDEO_DURATION.extractValue(tag);

        switch (durationCode) {
            case 1: return 60000L * 4;
            case 2: return 60000L * 20;
            case 3: return Long.MAX_VALUE;

            default: return Long.MIN_VALUE;
        }
    }
}
