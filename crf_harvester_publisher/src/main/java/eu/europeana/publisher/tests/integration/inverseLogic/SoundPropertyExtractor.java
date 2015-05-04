package eu.europeana.publisher.tests.integration.inverseLogic;

import eu.europeana.publisher.logic.TagEncoding;

public class SoundPropertyExtractor {
    //public static final Integer mask = (1 << 25) - 1;

    public static String getQuality(Integer tag) {
        final Integer qualityCode = TagEncoding.SOUND_QUALITY.extractValue(tag);

        if (1 == qualityCode) {
            return "true";
        }

        return "";
    }

    public static Long getDuration(Integer tag) {
        final Integer durationCode = TagEncoding.SOUND_DURATION.extractValue(tag);

        switch (durationCode) {
            case 1: return 60000L >> 1;
            case 2: return 60000L * 3;
            case 3: return 60000L * 4;
            case 4: return Long.MAX_VALUE;

            default: return Long.MIN_VALUE;
        }
    }
}
