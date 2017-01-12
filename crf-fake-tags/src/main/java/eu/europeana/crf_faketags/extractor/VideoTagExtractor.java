package eu.europeana.crf_faketags.extractor;

import eu.europeana.harvester.domain.VideoMetaInfo;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts the pure tags from a video resource and generates the fake tags.
 */
public class VideoTagExtractor {


    public static Integer getQualityCode(Integer height) {
        if (height == null || height < 576) return 0;
        else return 1;
    }

    public static Integer getQualityCode(Boolean videoQuality) {
        return BooleanUtils.isTrue(videoQuality) ? 1 : 0 ;
    }

    public static Integer getQualityCode(String videoQuality) {
        return StringUtils.containsIgnoreCase(videoQuality, "true") ? 1 : 0 ;
    }

    public static Integer getDurationCode(Long duration) {
        if (duration == null) return 0;
        else if (duration <= 240000) return 1;
        else if (duration <= 800000) return 2;
        else return 3;
    }

    public static Integer getDurationCode(String duration) {
        if (StringUtils.isBlank(duration)) return 0;
        else if(StringUtils.containsIgnoreCase(duration, "short")) return 1;
        else if(StringUtils.containsIgnoreCase(duration, "medium")) return 2;
        else if(StringUtils.containsIgnoreCase(duration, "long")) return 3;
        else return 0;
    }

    /**
     * Generates the filter/fake tags
     * @param videoMetaInfo the meta info object
     * @return the list of fake tags
     */
    public static List<Integer> getFilterTags(final VideoMetaInfo videoMetaInfo) {
        final List<Integer> filterTags = new ArrayList<>();
        final Integer mediaTypeCode = MediaTypeEncoding.VIDEO.getEncodedValue();

        if (null == videoMetaInfo.getMimeType() || null == videoMetaInfo.getHeight() || null == videoMetaInfo.getDuration()) {
            return new ArrayList<>();
        }

        final Integer qualityCode = getQualityCode(videoMetaInfo.getHeight());
        final Integer durationCode = getDurationCode(videoMetaInfo.getDuration());

        final Set<Integer> mimeTypeCodes = new HashSet<>();
        mimeTypeCodes.add(CommonTagExtractor.getMimeTypeCode(videoMetaInfo.getMimeType()));
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
                                          (mimeType << TagEncoding.MIME_TYPE.getBitPos()) |
                                          (quality  << TagEncoding.VIDEO_QUALITY.getBitPos()) |
                                          (duration << TagEncoding.VIDEO_DURATION.getBitPos());

                    filterTags.add(result);
                }
            }
        }

        return filterTags;
    }

    /**
     * Generates the list of facet tags.
     * @param videoMetaInfo the meta info object
     * @return the list of facet tags
     */
    public static List<Integer> getFacetTags(final VideoMetaInfo videoMetaInfo) {
        if (null == videoMetaInfo.getMimeType() || null == videoMetaInfo.getHeight() || null == videoMetaInfo.getDuration()) {
            return new ArrayList<>();
        }


        final List<Integer> facetTags = new ArrayList<>();

        final Integer mediaTypeCode = MediaTypeEncoding.VIDEO.getEncodedValue();

        Integer facetTag;

        final Integer mimeTypeCode = CommonTagExtractor.getMimeTypeCode(videoMetaInfo.getMimeType());
        facetTag = mediaTypeCode | (mimeTypeCode << TagEncoding.MIME_TYPE.getBitPos());
        facetTags.add(facetTag);

        final Integer qualityCode = getQualityCode(videoMetaInfo.getHeight());
        facetTag = mediaTypeCode | (qualityCode << TagEncoding.VIDEO_QUALITY.getBitPos());
        facetTags.add(facetTag);

        final Integer durationCode = getDurationCode(videoMetaInfo.getDuration());
        facetTag = mediaTypeCode | (durationCode << TagEncoding.VIDEO_DURATION.getBitPos());
        facetTags.add(facetTag);

        return facetTags;
    }

}
