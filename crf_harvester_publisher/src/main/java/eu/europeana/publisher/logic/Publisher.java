package eu.europeana.publisher.logic;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.*;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.WebResourceMetaInfoDAO;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDAOImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.domain.RetrievedDoc;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * It's responsible for the whole publishing process. It's the engine of the
 * publisher module.
 */
public class Publisher {

    private static final Logger LOG = LogManager.getLogger(Publisher.class.getName());

    private final PublisherConfig config;

    private DB sourceDB;

    private Integer LIMIT;

    final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDaoFromSource;

    final WebResourceMetaInfoDAO webResourceMetaInfoDAO;

    final SOLRWriter solrWriter;

    final long publisherStarteAt = System.currentTimeMillis();

    long publisherRecordsProcessed = 0;

    long publisherRecordsPublished = 0;

    public Publisher(PublisherConfig config) throws UnknownHostException {
        this.config = config;
        final Mongo sourceMongo = new Mongo(config.getSourceHost(), config.getSourcePort());

        if (!config.getSourceDBUsername().equals("")) {
            sourceDB = sourceMongo.getDB("admin");
            final Boolean auth = sourceDB.authenticate(config.getSourceDBUsername(),
                    config.getSourceDBPassword().toCharArray());
            if (!auth) {
                LOG.error("Mongo source auth error");
                System.exit(-1);
            }
        }

        sourceDB = sourceMongo.getDB(config.getSourceDBName());

        final Morphia sourceMorphia = new Morphia();
        final Datastore sourceDatastore = sourceMorphia.createDatastore(sourceMongo, config.getSourceDBName());

        sourceDocumentReferenceMetaInfoDaoFromSource = new SourceDocumentReferenceMetaInfoDaoImpl(sourceDatastore);

        DB targetDB;
        final Mongo targetMongo = new Mongo(config.getTargetHost(), config.getTargetPort());

        if (!config.getTargetDBUsername().equals("")) {
            targetDB = targetMongo.getDB("admin");
            final Boolean auth = targetDB.authenticate(config.getTargetDBUsername(),
                    config.getTargetDBPassword().toCharArray());
            if (!auth) {
                LOG.error("Mongo target auth error");
                System.exit(-1);
            }
        }

        final Morphia targetMorphia = new Morphia();
        final Datastore targetDatastore = targetMorphia.createDatastore(targetMongo, config.getTargetDBName());

        webResourceMetaInfoDAO = new WebResourceMetaInfoDAOImpl(targetDatastore);

        solrWriter = new SOLRWriter(config.getSolrURL());

        LIMIT = config.getBatch();
    }

    /**
     * The starting point of the publisher.
     */
    public void start() throws SolrServerException, IOException {

        Boolean done = false;
        HashMap<String, RetrievedDoc> retrievedDocsPerID;
        List<SourceDocumentReferenceMetaInfo> metaInfos;
        List<CRFSolrDocument> solrCandidateDocuments = new ArrayList<>();
        Integer skip = 0;

        do {
            solrCandidateDocuments.clear();
            // Retrieves the docs
            final long startTimeRetrieveDocs = System.currentTimeMillis();
            retrievedDocsPerID = retrieveStatisticsDocumentIdsThatMatch(skip);
            final long endTimeRetrieveDocs = System.currentTimeMillis();
            LOG.error("Retrieved: " + retrievedDocsPerID.size() + " docs" + " and it took " + (endTimeRetrieveDocs - startTimeRetrieveDocs) / 1000 + " seconds");

            DateTime lastSuccesfulPublish = null;

            // Find the earliest updatedAt (so we know where to continue from if the batch update fails)
            for (RetrievedDoc doc : retrievedDocsPerID.values()) {
                if (lastSuccesfulPublish == null) {
                    lastSuccesfulPublish = doc.getUpdatedAt();
                } else {
                    if (doc.getUpdatedAt().isBefore(lastSuccesfulPublish)) {
                        lastSuccesfulPublish = doc.getUpdatedAt();
                    }
                }
            }

            if (retrievedDocsPerID.size() == 0) {
                done = true;
            } else {
                // Sets the new value for the pagination
                skip += retrievedDocsPerID.size() < LIMIT ? retrievedDocsPerID.size() : LIMIT;

                // Retrieves the corresponding metaInformation objects
                final List<String> list = new ArrayList<>(retrievedDocsPerID.keySet());
                final long startTimeRetrieveMetaInfoDocs = System.currentTimeMillis();
                metaInfos = sourceDocumentReferenceMetaInfoDaoFromSource.read(list);
                final long endTimeRetrieveMetaInfoDocs = System.currentTimeMillis();
                LOG.error("Retrieved meta info docs: " + metaInfos.size() + " docs" + " and it took " + (endTimeRetrieveMetaInfoDocs - startTimeRetrieveMetaInfoDocs) / 1000 + " seconds");

                // Iterates over the meta info objects.
                for (SourceDocumentReferenceMetaInfo metaInfo : metaInfos) {
                    final String ID = metaInfo.getId();
                    final Integer mediaTypeCode = CommonTagExtractor.getMediaTypeCode(metaInfo);
                    Integer mimeTypeCode = null;

                    if (null != metaInfo.getAudioMetaInfo()) {
                        mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getAudioMetaInfo().getMimeType());
                    }

                    if (null != metaInfo.getVideoMetaInfo()) {
                        mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getVideoMetaInfo().getMimeType());
                    }

                    if (null != metaInfo.getImageMetaInfo()) {
                        mimeTypeCode = CommonTagExtractor.getMimeTypeCode(metaInfo.getImageMetaInfo().getMimeType());
                    }

                    System.out.println("mimeTypeCode is " + mimeTypeCode);

                    if (null != mimeTypeCode && mimeTypeCode == CommonTagExtractor.getMimeTypeCode("text/html")) {
                        LOG.error ("Skinping record with mimetype text/html. ID: " + ID);
                        continue;
                    }

                    // The new properties
                    Boolean isFulltext = false;
                    Boolean hasThumbnails = false;
                    Boolean hasMedia = false;
                    List<Integer> filterTags = new ArrayList<>();
                    List<Integer> facetTags = new ArrayList<>();

                    // Retrieves different type of properties depending on media type.
                    switch (mediaTypeCode) {
                        case 0:
                            break;
                        case 1:
                            final ImageMetaInfo imageMetaInfo = metaInfo.getImageMetaInfo();
                            filterTags = ImageTagExtractor.getFilterTags(imageMetaInfo);
                            facetTags = ImageTagExtractor.getFacetTags(imageMetaInfo);
                            hasMedia = true;
                            hasThumbnails = isImageWithThumbnail(imageMetaInfo);
                            if (hasThumbnails) {
                                hasMedia = false;
                            }

//                            System.out.println(retrievedDocsPerID.get(ID).getRecordId());
//                            System.out.println(imageMetaInfo.getColorSpace());
//                            System.out.println(imageMetaInfo.getFileFormat());
//                            System.out.println(imageMetaInfo.getFileSize());
//                            System.out.println(imageMetaInfo.getWidth());
//                            System.out.println(imageMetaInfo.getHeight());
//                            System.out.println(imageMetaInfo.getMimeType());
//                            System.out.println(imageMetaInfo.getOrientation());
//                            System.out.println(imageMetaInfo.getColorPalette().length);
                            break;
                        case 2:
                            final AudioMetaInfo audioMetaInfo = metaInfo.getAudioMetaInfo();
                            filterTags = SoundTagExtractor.getFilterTags(audioMetaInfo);
                            facetTags = SoundTagExtractor.getFacetTags(audioMetaInfo);

                            hasMedia = true;
                            break;
                        case 3:
                            final VideoMetaInfo videoMetaInfo = metaInfo.getVideoMetaInfo();
                            filterTags = VideoTagExtractor.getFilterTags(videoMetaInfo);
                            facetTags = VideoTagExtractor.getFacetTags(videoMetaInfo);

                            hasMedia = true;
                            break;
                        case 4:
                            final TextMetaInfo textMetaInfo = metaInfo.getTextMetaInfo();
                            isFulltext = textMetaInfo.getIsSearchable();

                            break;
                    }

                    // Creates the wrapping object for the new properties
                    final CRFSolrDocument CRFSolrDocument = new CRFSolrDocument(retrievedDocsPerID.get(ID).getRecordId(), isFulltext, hasThumbnails, hasMedia, filterTags, facetTags);
                    if (!CRFSolrDocument.getRecordId().toLowerCase().startsWith("/9200365/"))
                        solrCandidateDocuments.add(CRFSolrDocument);
                    else LOG.error("Skipping records that starts with /9200365/");
                }

                // Check which candidate documents have an existing SOLR document
                publisherRecordsProcessed += solrCandidateDocuments.size();

                final List<String> candidateDocumentIds = new ArrayList<String>();
                for (CRFSolrDocument doc : solrCandidateDocuments) {
                    candidateDocumentIds.add(doc.getRecordId());
                }

                final long startTimeCheckSolrExistence = System.currentTimeMillis();
                final Map<String, Boolean> documentIdToExistence = solrWriter.documentExists(candidateDocumentIds);
                int documentThatExist = 0;
                for (String id : documentIdToExistence.keySet()) {
                    if (documentIdToExistence.get(id) == true) {
                        documentThatExist++;
                    }
                }
                final long endTimeCheckSolrExistence = System.currentTimeMillis();
                LOG.error("Checked Solr document existence : " + documentThatExist + " in SOLR" + " out of  " + documentIdToExistence.keySet().size() + " in total and took" + (endTimeCheckSolrExistence - startTimeCheckSolrExistence) / 1000 + " seconds");

                // Generate the MongoDB documents that will be written in MongoDB : only the ones that have a corresponding SOLR document qualify
                final List<WebResourceMetaInfo> webResourceMetaInfosToUpdate = new ArrayList<>();
                for (SourceDocumentReferenceMetaInfo metaInfo : metaInfos) {
                    final String recordId = retrievedDocsPerID.get(metaInfo.getId()).getRecordId();
                    if ((documentIdToExistence.containsKey(recordId) == true) && (documentIdToExistence.get(recordId) == true)) {
                        final WebResourceMetaInfo webResourceMetaInfo = new WebResourceMetaInfo(metaInfo.getId(),
                                metaInfo.getImageMetaInfo(), metaInfo.getAudioMetaInfo(),
                                metaInfo.getVideoMetaInfo(), metaInfo.getTextMetaInfo());
                        webResourceMetaInfosToUpdate.add(webResourceMetaInfo);
                    }
                }

                // Filter the SOLR documents that will be written in SOLR : only the ones that have a coresponding SOLR document qualify
                final List<CRFSolrDocument> solrDocsToUpdate = new ArrayList<>();
                for (CRFSolrDocument doc : solrCandidateDocuments) {
                    if ((documentIdToExistence.containsKey(doc.getRecordId()) == true) && (documentIdToExistence.get(doc.getRecordId()) == true)) {
                        solrDocsToUpdate.add(doc);
                    }
                }

                publisherRecordsPublished += solrDocsToUpdate.size();


                // Writes the properties to the SOLR.
                final long startTimeSolrWrite = System.currentTimeMillis();
                final boolean successSolrUpdate = solrWriter.updateDocuments(solrDocsToUpdate);
                final long endTimeSolrWrite = System.currentTimeMillis();
                LOG.error("Updating: " + solrDocsToUpdate.size() + " SOLR docs." + " and it took " + (endTimeSolrWrite - startTimeSolrWrite) / 1000 + " seconds");

                // Writes the meta info to a separate MongoDB instance.
                if (successSolrUpdate) {
                    final long startTimeMongoWrite = System.currentTimeMillis();
                    webResourceMetaInfoDAO.create(webResourceMetaInfosToUpdate, WriteConcern.ACKNOWLEDGED);
                    final long endTimeMongoWrite = System.currentTimeMillis();
                    LOG.error("Write: " + webResourceMetaInfosToUpdate.size() + " meta infos" + " and it took " + (endTimeMongoWrite - startTimeMongoWrite) / 1000 + " seconds");
                }

                final long uptimeInSecs = (System.currentTimeMillis() - publisherStarteAt) / 1000;
                final long processingRate = publisherRecordsProcessed / uptimeInSecs;
                final long publishingRate = publisherRecordsPublished / uptimeInSecs;

                final long lastBatchDurationInSecs = (System.currentTimeMillis() - startTimeRetrieveMetaInfoDocs) / 1000;
                final long lastBatchProcessingRate = solrCandidateDocuments.size() / lastBatchDurationInSecs;
                final long lastBatchPublishingRate = solrDocsToUpdate.size() / lastBatchDurationInSecs;

                LOG.error("Global stats : " + " uptime : " + uptimeInSecs + " s" + " | process rate " + processingRate + " / s |  " + " | publish rate " + publishingRate + " / s ");
                LOG.error("Last batch stats : " + " duration : " + lastBatchDurationInSecs + " s" + " | process rate " + lastBatchProcessingRate + " / s |  " + " | publish rate " + lastBatchPublishingRate + " / s |  " + "Last succesful timestamp is : " + lastSuccesfulPublish);

                if (config.getStartTimestampFile() != null) {
                    final Path path = Paths.get(config.getStartTimestampFile());
                    Files.deleteIfExists(path);
                    Files.write(path, lastSuccesfulPublish.toString().getBytes());
                    LOG.error("Writting last succesfull timestamp " + lastSuccesfulPublish.toString() + " to file " + config.getStartTimestampFile());
                }
            }

        } while (!done);
    }

    /**
     * Retrieves a batch of documents
     *
     * @param skip how many documents will be skipped (needed for pagination)
     * @return the needed information from the retrieved documents.
     */
    private HashMap<String, RetrievedDoc> retrieveStatisticsDocumentIdsThatMatch(Integer skip) {
        final HashMap<String, RetrievedDoc> IDsWithType = new HashMap<>();

        final DBCollection sourceDocumentProcessingStatisticsCollection
                = sourceDB.getCollection("SourceDocumentProcessingStatistics");

        // Query construction
        BasicDBObject findQuery;
        if (null == config.getStartTimestamp()) {
            findQuery = new BasicDBObject("state", "SUCCESS");
        } else {
            LOG.error("Retrieving SourceDocumentProcessingStatistics gte than timestamp " + config.getStartTimestamp());

            findQuery = new BasicDBObject("updatedAt", new BasicDBObject("$gte", config.getStartTimestamp().toLocalDate().toDate())).append("state", "SUCCESS");
        }

        final BasicDBObject fields = new BasicDBObject("sourceDocumentReferenceId", true).append("httpResponseContentType", true).append("referenceOwner.recordId", true).append("_id", false).append("updatedAt", true);

        // Sort query results in ascending order by "updatedAt" field.
        final BasicDBObject sortOrder = new BasicDBObject();
        sortOrder.put("updatedAt", 1);

        DBCursor sourceDocumentProcessingStatisticsCursor
                = sourceDocumentProcessingStatisticsCollection.find(findQuery, fields).sort(sortOrder).skip(skip).limit(LIMIT);
        sourceDocumentProcessingStatisticsCursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        // Iterates over the loaded documents and takes the important information from them
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

        return IDsWithType;
    }

    /**
     * Checks if there were generated any thumbnail for this image
     *
     * @param imageMetaInfo the metainfo object
     * @return true if there is a thumbnail
     */
    private Boolean isImageWithThumbnail(ImageMetaInfo imageMetaInfo) {
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
