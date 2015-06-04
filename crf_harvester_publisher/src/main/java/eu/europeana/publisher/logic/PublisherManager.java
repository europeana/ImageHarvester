package eu.europeana.publisher.logic;

import com.codahale.metrics.*;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mongodb.*;
import eu.europeana.harvester.cluster.slave.SlaveMetrics;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.dao.PublisherEuropeanaDao;
import eu.europeana.publisher.dao.PublisherHarvesterDAO;
import eu.europeana.publisher.dao.SOLRWriter;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.domain.RetrievedDoc;
import eu.europeana.publisher.logic.extractor.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * It's responsible for the whole publishing process. It's the engine of the
 * publisher module.
 */
public class PublisherManager {
    private static final Logger LOG = LogManager.getLogger(PublisherManager.class.getName());

    private final PublisherConfig config;


    private final SOLRWriter solrWriter;
    private PublisherEuropeanaDao publisherEuropeanaDao;
    private PublisherHarvesterDAO publisherHarvesterDAO;

    private final PublisherMetrics publisherMetrics;

    public PublisherManager (PublisherConfig config) throws UnknownHostException {
        this.config = config;
        publisherMetrics = new PublisherMetrics();

        publisherEuropeanaDao = new PublisherEuropeanaDao(config.getSourceMongoConfig(), publisherMetrics);
        publisherHarvesterDAO = new PublisherHarvesterDAO(config.getTargetMongoConfig(), publisherMetrics);


        solrWriter = new SOLRWriter(config.getSolrURL());

        Slf4jReporter reporter = Slf4jReporter.forRegistry(PublisherMetrics.metricRegistry)
                                              .outputTo(org.slf4j.LoggerFactory.getLogger("metrics"))
                                              .convertRatesTo(TimeUnit.SECONDS)
                                              .convertDurationsTo(TimeUnit.MILLISECONDS).build();

        reporter.start(20, TimeUnit.SECONDS);

        if (null == config.getGraphiteConfig() || config.getGraphiteConfig().getServer().trim().isEmpty()) {
            return;
        }

        final InetSocketAddress addr = new InetSocketAddress(config.getGraphiteConfig().getServer(),
                                                             config.getGraphiteConfig().getPort());
        Graphite graphite = new Graphite(addr);
        GraphiteReporter reporter2 = GraphiteReporter.forRegistry(PublisherMetrics.metricRegistry)
                                                     .prefixedWith(config.getGraphiteConfig().getMasterId())
                                                     .convertRatesTo(TimeUnit.SECONDS)
                                                     .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL)
                                                     .build(graphite);
        reporter2.start(20, TimeUnit.SECONDS);
    }

    public void start () throws IOException, SolrServerException {
        try {
            publisherMetrics.startTotalTimer();
            startPublisher();
            publisherMetrics.stopTotalTimer();
        } finally {
            for (final Timer.Context context : publisherMetrics.getTimerContexts()) {
                context.close();
            }
        }
    }

    private void startPublisherNew() throws SolrServerException, IOException {
        DateTime currentTimestamp = config.getStartTimestamp();
        final DBCursor cursor = publisherEuropeanaDao.buildCursorForDocumentStatistics(config.getStartTimestamp().toDate());
        while (true) {

            final List<RetrievedDoc> retrievedDocs = publisherEuropeanaDao.retrieveDocumentStatistics(cursor, config.getBatch());

            if (null == retrievedDocs || retrievedDocs.isEmpty()) {
                break;
            }

            try {
                currentTimestamp = updateTimestamp(retrievedDocs, currentTimestamp);
            }
            catch (IOException e) {
                LOG.error ("Problem writing " + currentTimestamp + "to file: " + config.getStartTimestampFile(), e);
            }
            LOG.info ("New timestamp: " + currentTimestamp);

            final List<SourceDocumentReferenceMetaInfo> metaInfos = publisherEuropeanaDao.retrieveMetaInfo(retrievedDocs);
            final List<CRFSolrDocument> solrDocuments = FakeTagExtractor.extractTags(metaInfos);


        }
    }

    private DateTime updateTimestamp (List<RetrievedDoc> retrievedDocs, final DateTime currentTimestamp) throws
                                                                                                         IOException {
        // Find the latest updatedAt (so we know where to continue from if
        // the batch update fails)
        DateTime maxTimestamp = currentTimestamp;
        for (RetrievedDoc doc : retrievedDocs) {
            if (null == maxTimestamp) {
                maxTimestamp = doc.getUpdatedAt();
            }
            else if (maxTimestamp.isAfter(doc.getUpdatedAt())) {
                maxTimestamp = doc.getUpdatedAt();
            }
        }
        Files.write(Paths.get(config.getStartTimestampFile()), maxTimestamp.toString().getBytes());
        return currentTimestamp;
    }

    /**
     * The starting point of the publisher.
     */
    private void startPublisher () throws SolrServerException, IOException {
        Boolean done = false;
        HashMap<String, RetrievedDoc> retrievedDocsPerID;
        List<SourceDocumentReferenceMetaInfo> metaInfos;
        List<CRFSolrDocument> solrCandidateDocuments = new ArrayList<>();
        DateTime lastSuccesfulPublish = config.getStartTimestamp();


        do {
            publisherMetrics.startLoopBatchTimer();

            solrCandidateDocuments.clear();

            // Retrieves the docs
            retrievedDocsPerID = retrieveStatisticsDocumentIdsThatMatch(lastSuccesfulPublish);


            // Find the latest updatedAt (so we know where to continue from if
            // the batch update fails)
            DateTime lastSuccesfulPublishBeforeMax = lastSuccesfulPublish;
            for (RetrievedDoc doc : retrievedDocsPerID.values()) {
                if (lastSuccesfulPublish == null) {
                    lastSuccesfulPublish = doc.getUpdatedAt();
                }
                else {
                    if (doc.getUpdatedAt().isAfter(lastSuccesfulPublish)) {
                        lastSuccesfulPublish = doc.getUpdatedAt();
                    }
                }
            }

            // Advance with 1 minute if it's exactly the same (very unlikely).
            if (null != lastSuccesfulPublishBeforeMax && lastSuccesfulPublishBeforeMax.isEqual(lastSuccesfulPublish)) {
                lastSuccesfulPublish = lastSuccesfulPublish.plusMinutes(30);
            }

            if (retrievedDocsPerID.size() == 0) {
                done = true;
            }
            else {

                // Retrieves the corresponding metaInformation objects
                final List<String> list = new ArrayList<>(retrievedDocsPerID.keySet());

                publisherMetrics.startMongoGetMetaInfoTimer();
                metaInfos = sourceDocumentReferenceMetaInfoDaoFromSource.read(list);
                publisherMetrics.stopMongoGetMetaInfoTimer();

                publisherMetrics.logNumberOfMetaInfoDocumentsRetrived(metaInfos.size());

                publisherMetrics.startFakeTagsTimer();
                // Iterates over the meta info objects.

                publisherMetrics.stopFakeTagsTimer();

                // Check which candidate documents have an existing SOLR
                // document
                publisherMetrics.logNumberOfDocumentsProcessed(solrCandidateDocuments.size());

                final List<String> candidateDocumentIds = new ArrayList<String>();
                for (CRFSolrDocument doc : solrCandidateDocuments) {
                    candidateDocumentIds.add(doc.getRecordId());
                }

                publisherMetrics.startSolrReadIdsTimer();
                final Map<String, Boolean> documentIdToExistence = solrWriter.documentExists(candidateDocumentIds);
                publisherMetrics.stopSolrReadIdsTimer();

                int documentThatExist = 0;
                for (String id : documentIdToExistence.keySet()) {
                    if (documentIdToExistence.get(id) == true) {
                        documentThatExist++;
                    }
                }

                publisherMetrics.logNumberOfDocumentsThatExistInSolr(documentThatExist);

                // Generate the MongoDB documents that will be written in
                // MongoDB : only the ones that have a corresponding SOLR
                // document qualify
                final List<WebResourceMetaInfo> webResourceMetaInfosToUpdate = new ArrayList<>(documentThatExist);
                for (SourceDocumentReferenceMetaInfo metaInfo : metaInfos) {
                    final String recordId = retrievedDocsPerID.get(metaInfo.getId()).getRecordId();
                    final Boolean value = documentIdToExistence.get(recordId);
                    if (null != value && true == value) {
                        final WebResourceMetaInfo webResourceMetaInfo = new WebResourceMetaInfo(metaInfo.getId(),
                                                                                                metaInfo.getImageMetaInfo(),
                                                                                                metaInfo.getAudioMetaInfo(),
                                                                                                metaInfo.getVideoMetaInfo(),
                                                                                                metaInfo.getTextMetaInfo());
                        webResourceMetaInfosToUpdate.add(webResourceMetaInfo);
                    }
                }

                if (!solrCandidateDocuments.isEmpty()) {
                    // Filter the SOLR documents that will be written in SOLR :
                    // only the ones that have a coresponding SOLR document
                    // qualify
                    final List<CRFSolrDocument> solrDocsToUpdate = new ArrayList<>(documentThatExist);
                    for (CRFSolrDocument doc : solrCandidateDocuments) {
                        final Boolean value = documentIdToExistence.get(doc.getRecordId());
                        if (null != value && true == value) {
                            solrDocsToUpdate.add(doc);
                        }
                    }

                    publisherMetrics.logNumberOfDocumentsPublished(solrDocsToUpdate.size());

                    // Writes the properties to the SOLR.
                    publisherMetrics.startSolrUpdateTimer();
                    final boolean successSolrUpdate = solrWriter.updateDocuments(solrDocsToUpdate);
                    publisherMetrics.stopSolrUpdateTimer();

                    // Writes the meta info to a separate MongoDB instance.
                    if (successSolrUpdate) {
                        publisherMetrics.startMongoWriteTimer();
                        webResourceMetaInfoDAO.create(webResourceMetaInfosToUpdate, WriteConcern.ACKNOWLEDGED);
                        publisherMetrics.stopMongoWriteTimer();
                    }

                    if (config.getStartTimestampFile() != null) {
                        final Path path = Paths.get(config.getStartTimestampFile());
                        Files.deleteIfExists(path);
                        Files.write(path, lastSuccesfulPublish.toString().getBytes());
                        LOG.error("Writing last succesfull timestamp " + lastSuccesfulPublish
                                                                                 .toString() + " to file " + config.getStartTimestampFile());
                    }

                }
                else {
                    LOG.error("No records found in Solr");
                }
            }
            publisherMetrics.stopLoopBatchTimer();
        } while (!done);
    }

    /**
     * Retrieves a batch of documents
     *
     * @return the needed information from the retrieved documents.
     */
    private HashMap<String, RetrievedDoc> retrieveStatisticsDocumentIdsThatMatch (final DateTime startTimeStamp) {
        publisherMetrics.startMongoGetDocStatTimer();

        final HashMap<String, RetrievedDoc> IDsWithType = new HashMap<>();

        final DBCollection sourceDocumentProcessingStatisticsCollection = sourceDB.getCollection("SourceDocumentProcessingStatistics");

        // Query construction
        BasicDBObject findQuery;
        if (null == startTimeStamp) {
            //	findQuery = new BasicDBObject("state", "SUCCESS");
            findQuery = new BasicDBObject();

        }
        else {
            LOG.error("Retrieving SourceDocumentProcessingStatistics gt than timestamp " + startTimeStamp);

            findQuery = new BasicDBObject();
            findQuery.put("updatedAt", new BasicDBObject("$gt", startTimeStamp.toDate()));
            // findQuery.put("state", "SUCCESS"); -- COMMENTED BY PAUL

        }

        final BasicDBObject fields = new BasicDBObject("sourceDocumentReferenceId", true)
                                             .append("httpResponseContentType", true)
                                             .append("referenceOwner.recordId", true).append("_id", false)
                                             .append("updatedAt", true);

        // Sort query results in ascending order by "updatedAt" field.
        final BasicDBObject sortOrder = new BasicDBObject();
        sortOrder.put("updatedAt", 1);

        DBCursor sourceDocumentProcessingStatisticsCursor = sourceDocumentProcessingStatisticsCollection
                                                                    .find(findQuery, fields).sort(sortOrder)
                                                                    .limit(LIMIT)
                                                                    .addOption(Bytes.QUERYOPTION_NOTIMEOUT);


        LOG.error("Executing MongoDB query to retrieve stats " + sourceDocumentProcessingStatisticsCursor.getQuery()
                                                                                                         .toString());

        // Iterates over the loaded documents and takes the important
        // information from them
        LOG.error("Retrieving SourceDocumentProcessingStatistics from cursor with size " + sourceDocumentProcessingStatisticsCursor
                                                                                                   .size());

        while (sourceDocumentProcessingStatisticsCursor.hasNext()) {
            final BasicDBObject item = (BasicDBObject) sourceDocumentProcessingStatisticsCursor.next();
            final DateTime updatedAt = new DateTime(item.getDate("updatedAt"));
            final String sourceDocumentReferenceId = item.getString("sourceDocumentReferenceId");
            final BasicDBObject referenceOwnerTemp = (BasicDBObject) item.get("referenceOwner");
            final String recordId = referenceOwnerTemp.getString("recordId");
            final String temp = item.getString("httpResponseContentType");
            String type = "";
            if (temp != null) {
                final String[] parts = temp.split(";");
                type = parts[0];
            }

            if (type.contains(",")) {
                type = type.substring(0, type.indexOf(","));
            }

            final RetrievedDoc retrievedDoc = new RetrievedDoc(type, recordId, updatedAt);
            IDsWithType.put(sourceDocumentReferenceId, retrievedDoc);
        }

        publisherMetrics.stopMongoGetDocStatTimer();
        return IDsWithType;
    }

    /**
     * Checks if there were generated any thumbnail for this image
     *
     * @param imageMetaInfo the metainfo object
     * @return true if there is a thumbnail
     */
    private Boolean isImageWithThumbnail (ImageMetaInfo imageMetaInfo) {
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
