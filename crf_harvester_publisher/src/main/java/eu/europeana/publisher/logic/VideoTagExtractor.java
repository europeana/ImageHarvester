package eu.europeana.publisher.logic;

import eu.europeana.harvester.domain.VideoMetaInfo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts the pure tags from a video resource and generates the fake tags.
 */
public class VideoTagExtractor {

    private static Integer getQualityCode(Integer height) {
        if(height == null || height<576) {
            return 0;
        }

        return 1;
    }

    private static Integer getDurationCode(Long duration) {
        if(duration == null) {
            return 0;
        }
        final Long temp = duration/60000;
        if(temp <= 4) {
            return 1;
        }
        if(temp <= 20) {
            return 2;
        }

        return 3;
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
