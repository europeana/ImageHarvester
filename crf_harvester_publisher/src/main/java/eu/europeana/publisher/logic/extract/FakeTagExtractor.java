package eu.europeana.publisher.logic.extract;

import eu.europeana.crf_faketags.extractor.CommonTagExtractor;
import eu.europeana.crf_faketags.extractor.ImageTagExtractor;
import eu.europeana.crf_faketags.extractor.SoundTagExtractor;
import eu.europeana.crf_faketags.extractor.VideoTagExtractor;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.SkippedRecords;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by salexandru on 04.06.2015.
 */
public class FakeTagExtractor {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FakeTagExtractor.class.getName());

    public static List<CRFSolrDocument> extractTags(final Collection<HarvesterDocument> harvesterDocuments,final String publishingBatchId) {
        final List<CRFSolrDocument> solrDocuments = new ArrayList<>();

        for (final HarvesterDocument document : harvesterDocuments) {
            if (DocumentReferenceTaskType.CHECK_LINK.equals(document.getTaskType())) {
                System.out.println ("check link document generation");
                final CRFSolrDocument CRFSolrDocument = new CRFSolrDocument(
                                                                                   document.getReferenceOwner().getRecordId(),
                                                                                   false,
                                                                                   false,
                                                                                   false,
                                                                                   null,
                                                                                   null,
                                                                                   document.getUrlSourceType(),
                                                                                   document.getUrl(),
                                                                                   ProcessingJobRetrieveSubTaskState.SUCCESS.equals(document.getSubTaskStats().getRetrieveState()),
                                                                                   document.getTaskType()
                );
                solrDocuments.add(CRFSolrDocument);
                continue;
            }


            final SourceDocumentReferenceMetaInfo metaInfo = document.getSourceDocumentReferenceMetaInfo();

            final String ID = metaInfo.getId();
            final Integer mediaTypeCode = CommonTagExtractor.getMediaTypeCode(metaInfo);
            Integer mimeTypeCode = null;

            System.out.println("Normal tag generation");

            if (null == metaInfo.getAudioMetaInfo() && null == metaInfo.getImageMetaInfo() &&
                    null == metaInfo.getVideoMetaInfo() && null == metaInfo.getTextMetaInfo()) {

                PublisherMetrics.Publisher.Batch.totalNumberOfDocumentsWithoutMetaInfo.inc();
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR, publishingBatchId, null, document.getReferenceOwner()),
                        "MetaInfo missing from CRF entry. Skipping generating any tags.");
                continue;
            }

            if (null != metaInfo.getAudioMetaInfo() && null != metaInfo.getAudioMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getAudioMetaInfo().getMimeType());
            } else if (null != metaInfo.getVideoMetaInfo() && null != metaInfo.getVideoMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getVideoMetaInfo().getMimeType());
            } else if (null != metaInfo.getImageMetaInfo() && null != metaInfo.getImageMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getImageMetaInfo().getMimeType());
            } else if (null != metaInfo.getTextMetaInfo() && null != metaInfo.getTextMetaInfo().getMimeType()) {
                mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getTextMetaInfo().getMimeType());
            }

            if (null == mimeTypeCode) {
                PublisherMetrics.Publisher.Batch.totalNumberOfInvalidMimetypes.inc();
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR, publishingBatchId, null, document.getReferenceOwner()),
                        "Mime-Type is missing (is null) for CRF entry with meta-info ID {} . No Mime-Type tags will be generated.", ID);
            } else if (mimeTypeCode == CommonTagExtractor.getMimeTypeCode("text/html")) {
                PublisherMetrics.Publisher.Batch.totalNumberOfInvalidMimetypes.inc();
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR, publishingBatchId, null, document.getReferenceOwner()),
                        "Mime-Type is text/html for CRF entry with meta-info ID {}. The entire CRF entry will be skipped..", ID);
                continue;
            }

            // The new properties
            Boolean isFulltext = false;
            Boolean hasThumbnails = false;
            Boolean hasMedia;
            List<Integer> filterTags = new ArrayList<>();
            List<Integer> facetTags = new ArrayList<>();

            System.out.print(document.getSubTaskStats().getMetaExtractionState() + " ");
            System.out.print(mimeTypeCode + " ");

            hasMedia = ProcessingJobSubTaskState.SUCCESS.equals(document.getSubTaskStats().getMetaExtractionState()) &&
                       null != mimeTypeCode;

            System.out.println(hasMedia);

            // Retrieves different type of properties depending on media
            // type.
            switch (mediaTypeCode) {
                case 0:
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR, publishingBatchId, null, document.getReferenceOwner()),
                            "CRF entry had a media type code 0. The entire CRF entry will be skipped..", ID);
                    continue;

                case 1:
                    final ImageMetaInfo imageMetaInfo = metaInfo.getImageMetaInfo();
                    filterTags = ImageTagExtractor.getFilterTags(imageMetaInfo);
                    facetTags = ImageTagExtractor.getFacetTags(imageMetaInfo);
                    hasThumbnails = ProcessingJobSubTaskState.SUCCESS == document.getSubTaskStats().getThumbnailGenerationState() &&
                                    ProcessingJobSubTaskState.SUCCESS == document.getSubTaskStats().getThumbnailStorageState() &&
                                    (URLSourceType.OBJECT == document.getUrlSourceType() ||
                                     URLSourceType.ISSHOWNBY == document.getUrlSourceType());

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

            final CRFSolrDocument CRFSolrDocument = new CRFSolrDocument(
                    document.getReferenceOwner().getRecordId(),
                    isFulltext,
                    hasThumbnails,
                    hasMedia,
                    filterTags,
                    facetTags,
                    document.getUrlSourceType(),
                    document.getUrl(),
                    ProcessingJobRetrieveSubTaskState.SUCCESS.equals(document.getSubTaskStats().getRetrieveState()),
                    document.getTaskType()
            );


            if (!CRFSolrDocument.getRecordId().toLowerCase().startsWith(SkippedRecords.id)) {
                solrDocuments.add(CRFSolrDocument);
            } else {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR, publishingBatchId, null, document.getReferenceOwner()),
                        "Skipping record that starts with ID {}. The entire CRF entry will be skipped.", SkippedRecords.id);
            }
        }
        return solrDocuments;
    }
}
