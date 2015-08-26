package eu.europeana.publisher.logic.extract;

import eu.europeana.crf_faketags.extractor.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.SkippedRecords;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.HarvesterRecord;
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

    private static boolean hasLandingpage(final HarvesterDocument edmIsShownAt) {
        return null != edmIsShownAt &&
               ProcessingJobRetrieveSubTaskState.SUCCESS == edmIsShownAt.getSubTaskStats().getRetrieveState();
    }

    private static boolean hasThumbnail (HarvesterDocument edmDocument) {
        return null != edmDocument &&
               ProcessingJobSubTaskState.SUCCESS == edmDocument.getSubTaskStats().getThumbnailGenerationState() &&
               ProcessingJobSubTaskState.SUCCESS == edmDocument.getSubTaskStats().getThumbnailStorageState();
    }

    private static boolean hasThumbnail (HarvesterDocument edmIsShownByDocument, HarvesterDocument edmObjectDocument) {
        return hasThumbnail(edmIsShownByDocument) || hasThumbnail(edmObjectDocument);
    }

    private static CRFSolrDocument generateTags (final CRFSolrDocument solrDocument,
                                                 final ReferenceOwner owner,
                                                 final Collection<SourceDocumentReferenceMetaInfo> metaInfoList,
                                                 final String publishingBatchId) {
        boolean hasMedia = false;
        boolean hasFullText = false;
        final List<Integer> facetTags = new ArrayList<>();
        final List<Integer> filterTags = new ArrayList<>();

        for (final SourceDocumentReferenceMetaInfo metaInfo : metaInfoList) {
            final String ID = metaInfo.getId();
            final Integer mediaTypeCode = CommonTagExtractor.getMediaTypeCode(metaInfo);
            Integer mimeTypeCode = null;


            if (null == metaInfo.getAudioMetaInfo() && null == metaInfo.getImageMetaInfo() &&
                null == metaInfo.getVideoMetaInfo() && null == metaInfo.getTextMetaInfo()) {

                PublisherMetrics.Publisher.Batch.totalNumberOfDocumentsWithoutMetaInfo.inc();
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR,
                                                           publishingBatchId, null,
                                                           owner
                                                          ),
                          "MetaInfo missing from CRF entry. Skipping generating any tags.");
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
                PublisherMetrics.Publisher.Batch.totalNumberOfInvalidMimeTypes.inc();
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR,
                                                           publishingBatchId, null, owner),
                          "Mime-Type is missing (is null) for CRF entry with meta-info ID {} . No Mime-Type tags " +
                                  "will be generated.",
                          ID);
            }
            else if (mimeTypeCode == CommonTagExtractor.getMimeTypeCode("text/html")) {
                PublisherMetrics.Publisher.Batch.totalNumberOfInvalidMimeTypes.inc();
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR,
                                                           publishingBatchId, null, owner),
                          "Mime-Type is text/html for CRF entry with meta-info ID {}. The entire CRF entry will " +
                                  "be skipped..",
                          ID);
                continue;
            }

            hasMedia = true;

            switch (MediaTypeEncoding.valueOf(mediaTypeCode)) {
                case IMAGE:
                    facetTags.addAll(ImageTagExtractor.getFacetTags(metaInfo.getImageMetaInfo()));
                    filterTags.addAll(ImageTagExtractor.getFilterTags(metaInfo.getImageMetaInfo()));
                    break;

                case VIDEO:
                    facetTags.addAll(VideoTagExtractor.getFacetTags(metaInfo.getVideoMetaInfo()));
                    filterTags.addAll(VideoTagExtractor.getFilterTags(metaInfo.getVideoMetaInfo()));
                    break;

                case AUDIO:
                    facetTags.addAll(SoundTagExtractor.getFacetTags(metaInfo.getAudioMetaInfo()));
                    filterTags.addAll(SoundTagExtractor.getFilterTags(metaInfo.getAudioMetaInfo()));
                    break;

                case TEXT:
                    hasFullText = hasFullText ||
                                  metaInfo.getTextMetaInfo().getIsSearchable();
                    break;

                default:
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR,
                                                               publishingBatchId, null, owner),
                              "Unknown mimetype for metainfo id: {} from record id {}", ID, owner.getRecordId());
            }
        }

        return solrDocument.withFacetTags(facetTags)
                           .withFilterTags(filterTags)
                           .withHasMedia(hasMedia)
                           .withIsFullText(hasFullText);
    }

    public static List<CRFSolrDocument> extractTags(final Collection<HarvesterRecord> harvesterRecords,final String publishingBatchId) {
        final List<CRFSolrDocument> solrDocuments = new ArrayList<>();

        for (final HarvesterRecord record : harvesterRecords) {
            if (record.getRecordId().startsWith(SkippedRecords.id)) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING_TAG_EXTRACTOR,
                                                           publishingBatchId, null, record.getReferenceOwner()),
                          "Skipping record that starts with ID {}. The entire CRF entry will be skipped.",
                          SkippedRecords.id);
                continue;
            }

            CRFSolrDocument solrDocument = new CRFSolrDocument(record.getRecordId());

            solrDocument = solrDocument.withHasLandingpage(hasLandingpage(record.getEdmIsShownAtDocument()));

            solrDocument = solrDocument.withHasThumbnails(hasThumbnail(record.getEdmIsShownByDocument(),
                                                                       record.getEdmObjectDocument()
                                                                      )
            );

            if (record.updateEdmObject()) {
                solrDocument = solrDocument.withProviderEdmObject(record.newEdmObjectUrl());
            }

            solrDocument = generateTags(solrDocument,
                                        record.getReferenceOwner(),
                                        record.getUniqueMetainfos(),
                                        publishingBatchId);

            solrDocuments.add(solrDocument);

        }
        return solrDocuments;
    }


}
