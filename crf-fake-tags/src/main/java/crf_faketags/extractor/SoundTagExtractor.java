package crf_faketags.extractor;

import eu.europeana.harvester.domain.AudioMetaInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts the pure tags from an audio resource and generates the fake tags.
 */
public class SoundTagExtractor {

    private static Integer getQualityCode(Integer bitDepth, Integer sampleRate, String fileFormat) {
        if(bitDepth != null && sampleRate != null && bitDepth >= 16 && sampleRate >= 44100) {
            return 1;
        }

        if(fileFormat != null && (fileFormat.equalsIgnoreCase("alac") || fileFormat.equalsIgnoreCase("flac") ||
                fileFormat.equalsIgnoreCase("ape") || fileFormat.equalsIgnoreCase("shn") ||
                fileFormat.equalsIgnoreCase("wav") || fileFormat.equalsIgnoreCase("wma") ||
                fileFormat.equalsIgnoreCase("aiff") || fileFormat.equalsIgnoreCase("dsd"))) {
            return 1;
        }

        return 0;
    }

    private static Integer getDurationCode(Long duration) {
        if(duration == null) {
            return 0;
        }
        final Double temp = duration/60000.0;
        if(temp.compareTo(0.5) < 0) {

            return 1;
        }
        if(temp.compareTo(3.0) < 0) {
            return 2;
        }
        if(temp.compareTo(6.0) < 0) {
            return 3;
        }

        return 4;
    }

    /**
     * Generates the filter/fake tags
     * @param audioMetaInfo the meta info object
     * @return the list of fake tags
     */
    public static List<Integer> getFilterTags(final AudioMetaInfo audioMetaInfo) {
        final List<Integer> filterTags = new ArrayList<>();
        final Integer mediaTypeCode = MediaTypeEncoding.AUDIO.getEncodedValue();

        if (null == audioMetaInfo.getMimeType() || null == audioMetaInfo.getDuration() ||
            null == audioMetaInfo.getBitDepth() || null == audioMetaInfo.getSampleRate() ||
            null == audioMetaInfo.getFileFormat()) {
            return new ArrayList<>();
        }

        final Integer qualityCode = getQualityCode(audioMetaInfo.getBitDepth(), audioMetaInfo.getSampleRate(), audioMetaInfo.getFileFormat());
        final Integer durationCode = getDurationCode(audioMetaInfo.getDuration());

        final Set<Integer> mimeTypeCodes = new HashSet<>();
        mimeTypeCodes.add(CommonTagExtractor.getMimeTypeCode(audioMetaInfo.getMimeType()));
        mimeTypeCodes.add(0);

        final Set<Integer> qualityCodes = new HashSet<>();
        qualityCodes.add(qualityCode);
        qualityCodes.add(0);

        final Set<Integer> durationCodes = new HashSet<>();
        durationCodes.add(durationCode);
        durationCodes.add(0);

        for (Integer mimeType : mimeTypeCodes) {
            for (Integer quality : qualityCodes) {
                for (Integer duration : durationCodes) {
                    final Integer result = mediaTypeCode |
                                           (mimeType << TagEncoding.MIME_TYPE.getBitPos())  |
                                           (quality  << TagEncoding.SOUND_QUALITY.getBitPos()) |
                                           (duration << TagEncoding.SOUND_DURATION.getBitPos());

                    filterTags.add(result);

//                  System.out.println(result);
//                  System.out.println(mediaTypeCode + " " + mimeType + " " + fileSize + " " + colorSpace + " " + aspectRatio + " " + color);
//                  System.out.println(Integer.toBinaryString(result));
                }
            }
        }

        return filterTags;
    }

    /**
     * Generates the list of facet tags.
     * @param audioMetaInfo the meta info object
     * @return the list of facet tags
     */
    public static List<Integer> getFacetTags(final AudioMetaInfo audioMetaInfo) {
        if (null == audioMetaInfo.getMimeType() || null == audioMetaInfo.getDuration() ||
            null == audioMetaInfo.getBitDepth() || null == audioMetaInfo.getSampleRate() ||
            null == audioMetaInfo.getFileFormat()) {
            return new ArrayList<>();
        }

        final List<Integer> facetTags = new ArrayList<>();
        final Integer mediaTypeCode = MediaTypeEncoding.AUDIO.getEncodedValue();

        Integer facetTag;

        final Integer mimeTypeCode = CommonTagExtractor.getMimeTypeCode(audioMetaInfo.getMimeType());
        facetTag = mediaTypeCode | (mimeTypeCode << TagEncoding.MIME_TYPE.getBitPos());
        facetTags.add(facetTag);

        final Integer qualityCode = getQualityCode(audioMetaInfo.getBitDepth(), audioMetaInfo.getSampleRate(), audioMetaInfo.getFileFormat());
        facetTag = mediaTypeCode | (qualityCode << TagEncoding.SOUND_QUALITY.getBitPos());
        facetTags.add(facetTag);

        final Integer durationCode = getDurationCode(audioMetaInfo.getDuration());
        facetTag = mediaTypeCode | (durationCode << TagEncoding.SOUND_DURATION.getBitPos());
        facetTags.add(facetTag);

        return facetTags;
    }

}
