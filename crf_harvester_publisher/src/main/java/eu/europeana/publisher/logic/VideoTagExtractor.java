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
     * @param mimeTypeCode the mimetype of the resource
     * @return the list of fake tags
     */
    public static List<Integer> getFilterTags(final VideoMetaInfo videoMetaInfo, Integer mimeTypeCode) {
        final List<Integer> filterTags = new ArrayList<>();
        final Integer mediaTypeCode = 3;

        if(videoMetaInfo.getMimeType() != null) {
            mimeTypeCode = CommonTagExtractor.getMimeTypeCode(videoMetaInfo.getMimeType());
        }
        final Integer qualityCode = getQualityCode(videoMetaInfo.getHeight());
        final Integer durationCode = getDurationCode(videoMetaInfo.getDuration());

        final Set<Integer> mimeTypeCodes = new HashSet<>();
        mimeTypeCodes.add(mimeTypeCode);
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
                    final Integer result = mediaTypeCode<<25 | mimeType<<15 | quality<<13 | duration<<10;

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
     * @param mimeTypeCode the mimetype of the resource
     * @return the list of facet tags
     */
    public static List<Integer> getFacetTags(final VideoMetaInfo videoMetaInfo, Integer mimeTypeCode) {
        final List<Integer> facetTags = new ArrayList<>();

        final Integer mediaTypeCode = 3;

        Integer facetTag;

        if(videoMetaInfo.getMimeType() != null) {
            mimeTypeCode = CommonTagExtractor.getMimeTypeCode(videoMetaInfo.getMimeType());
            facetTag = mediaTypeCode<<25 | mimeTypeCode<<15;
            facetTags.add(facetTag);
        }

        final Integer qualityCode = getQualityCode(videoMetaInfo.getHeight());
        facetTag = mediaTypeCode<<25 | qualityCode<<13;
        facetTags.add(facetTag);

        final Integer durationCode = getDurationCode(videoMetaInfo.getDuration());
        facetTag = mediaTypeCode<<25 | durationCode<<10;
        facetTags.add(facetTag);

        return facetTags;
    }

}
