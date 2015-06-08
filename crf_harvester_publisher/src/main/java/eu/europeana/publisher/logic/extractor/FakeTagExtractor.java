package eu.europeana.publisher.logic.extractor;

import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.RetrievedDoc;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by salexandru on 04.06.2015.
 */
public class FakeTagExtractor {
    private static final Logger LOG = LogManager.getLogger(FakeTagExtractor.class.getName());

    public static List<CRFSolrDocument> extractTags (final Map<String, RetrievedDoc> retrievedDocs,
                                                      final List<SourceDocumentReferenceMetaInfo> metaInfos, final
                                                     PublisherMetrics publisherMetrics
                                                    ) {

        for (SourceDocumentReferenceMetaInfo metaInfo : metaInfos) {
            final String ID = metaInfo.getId();
            final Integer mediaTypeCode = CommonTagExtractor.getMediaTypeCode(metaInfo);
            Integer mimeTypeCode = null;

            if (null == metaInfo.getAudioMetaInfo() && null == metaInfo.getImageMetaInfo() &&
                        null == metaInfo.getVideoMetaInfo() && null == metaInfo.getTextMetaInfo()) {
                publisherMetrics.incTotalNumberOfDocumentsWithoutMetaInfo();
                LOG.error("Record : " + ID + " with metaInfo id: " + metaInfo.getId() + " has no metainfo");
                continue;
            }

            if (null != metaInfo.getAudioMetaInfo() && null != metaInfo.getAudioMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getAudioMetaInfo().getMimeType());
            }
            else if (null != metaInfo.getVideoMetaInfo() && null != metaInfo.getVideoMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getVideoMetaInfo().getMimeType());
            }
            else if (null != metaInfo.getImageMetaInfo() && null != metaInfo.getImageMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getImageMetaInfo().getMimeType());
            }
            else if (null != metaInfo.getTextMetaInfo() && null != metaInfo.getTextMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getTextMetaInfo().getMimeType());
            }

            if (null == mimeTypeCode) {
                LOG.error("Mime-Type null for document id: " + ID);
                publisherMetrics.incTotalNumberOfInvalidMimetypes();
            }
            else if (mimeTypeCode == CommonTagExtractor.getMimeTypeCode("text/html")) {
                LOG.error("Skipping record with mimetype text/html. ID: " + ID);
                publisherMetrics.incTotalNumberOfInvalidMimetypes();
                continue;
            }

            // The new properties
            Boolean isFulltext = false;
            Boolean hasThumbnails = false;
            Boolean hasMedia = true;
            List<Integer> filterTags = new ArrayList<>();
            List<Integer> facetTags = new ArrayList<>();

            // Retrieves different type of properties depending on media
            // type.
            switch (mediaTypeCode) {
                case 0:
                    LOG.error("RecordID " + ID + " with metainfo id: " + metaInfo.getId() + " has " +
                                      "mediaTypeCode 0 skipping.");
                    continue;

                case 1:
                    final ImageMetaInfo imageMetaInfo = metaInfo.getImageMetaInfo();
                    filterTags = ImageTagExtractor.getFilterTags(imageMetaInfo);
                    facetTags = ImageTagExtractor.getFacetTags(imageMetaInfo);
                    hasThumbnails = isImageWithThumbnail(imageMetaInfo);

                    if (hasThumbnails) {
                        hasMedia = false;
                    }

                    break;

                case 2:
                    final AudioMetaInfo audioMetaInfo = metaInfo.getAudioMetaInfo();
                    filterTags = SoundTagExtractor.getFilterTags(audioMetaInfo);
                    facetTags = SoundTagExtractor.getFacetTags(audioMetaInfo);
                    break;

                case 3:
                    final VideoMetaInfo videoMetaInfo = metaInfo.getVideoMetaInfo();
                    filterTags = VideoTagExtractor.getFilterTags(videoMetaInfo);
                    facetTags = VideoTagExtractor.getFacetTags(videoMetaInfo);
                    break;

                case 4:
                    final TextMetaInfo textMetaInfo = metaInfo.getTextMetaInfo();
                    isFulltext = textMetaInfo.getIsSearchable();
                    break;
            }

        }
    }

    /**
     * Checks if there were generated any thumbnail for this image
     *
     * @param imageMetaInfo the metainfo object
     * @return true if there is a thumbnail
     */
    private static boolean isImageWithThumbnail (ImageMetaInfo imageMetaInfo) {
        if (imageMetaInfo.getColorSpace() != null) {
            return false;
        }
        if (imageMetaInfo.getFileFormat() != null) {
            return false;
        }
        if (imageMetaInfo.getFileSize() != null) {
            return false;
        }
        if (imageMetaInfo.getHeight() != null) {
            return false;
        }
        if (imageMetaInfo.getWidth() != null) {
            return false;
        }
        if (imageMetaInfo.getMimeType() != null) {
            return false;
        }
        if (imageMetaInfo.getOrientation() != null) {
            return false;
        }
        if (imageMetaInfo.getColorPalette() == null || imageMetaInfo.getColorPalette().length == 0) {
            return false;
        }

        return true;
    }
}
