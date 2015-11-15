package eu.europeana.publisher.dao;

import com.codahale.metrics.Timer;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonMap;

/**
 * Writes additional fields/properties to SOLR
 */
public class SOLRWriter {

    /**
     * The maximum number of ID's that can be present in a SOLR search query.
     * Important because of the limitations of the HTTP URL length.
     */
    private static final int MAX_NUMBER_OF_IDS_IN_SOLR_QUERY = 1000;

    /**
     * The maximum number of docs that can be present in a SOLR update query.
     * Important because of the limitations of the HTTP URL length.
     */
    private static final int MAX_NUMBER_OF_DOCS_IN_SOLR_UPDATE = 50*1000;

    /*
     *  Connection timeout for solr queries. Time unit is milliseconds
     */
    private static final int CONNECTION_TIMEOUT = 5000;

    private static final int MAX_RETRIES = 1;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
    private final String solrUrl;
    private final Boolean solrCommitEnabled;
    private final String connectionId;


    public SOLRWriter (final DBTargetConfig solrConfig) {
        if (null == solrConfig || null == solrConfig.getSolrUrl() || solrConfig.getSolrUrl().trim().isEmpty()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR),
                    "Cannot initialise the SOLR persistence as the provided Sorl base url is null.Exiting.");
            throw new IllegalArgumentException("Solr Url cannot be null");
        }

        this.connectionId = solrConfig.getName();
        this.solrUrl = solrConfig.getSolrUrl();
        this.solrCommitEnabled = solrConfig.getSolrCommitEnabled();
    }

    private SolrClient createServer (String publishingBatchId) {
        int retry = 0;
        while (true) {
            try {
                LOG.info(LoggingComponent
                                 .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId, null,
                                                  null),
                         "Trying to connect to the solr server: " + solrUrl + ". Retry #" + retry);
                final RequestConfig.Builder requestBuilder = RequestConfig.custom()
                                                                          .setConnectTimeout(CONNECTION_TIMEOUT)
                                                                          .setConnectionRequestTimeout(CONNECTION_TIMEOUT);
                final HttpClientBuilder clientBuilder = HttpClientBuilder.create()
                                                                         .setDefaultRequestConfig(requestBuilder
                                                                                                          .build());
                final HttpSolrClient server = new HttpSolrClient(solrUrl, clientBuilder.build(), new BinaryResponseParser());
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                           publishingBatchId, null, null),
                          "Connected successfully to solr: " + solrUrl + ". Retry #" + retry);
                return server;
            }
            catch (Exception e) {
                ++retry;
                if (retry > MAX_RETRIES) throw e;
            }
        }
    }

    public String getSolrUrl() {return solrUrl;}


    public void updateDocuments (List<CRFSolrDocument> newDocs,final String publishingBatchId) throws IOException, SolrServerException {
        LOG.warn(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                "Preparing to split SOLR update of {} docs in chunks of {}",newDocs.size(),MAX_NUMBER_OF_DOCS_IN_SOLR_UPDATE);

        for (int docsStartChunkIndex = 0; docsStartChunkIndex <= newDocs.size(); docsStartChunkIndex += MAX_NUMBER_OF_DOCS_IN_SOLR_UPDATE) {
            final int endOfArray = Math.min(newDocs.size(), docsStartChunkIndex + MAX_NUMBER_OF_DOCS_IN_SOLR_UPDATE);
            final List<CRFSolrDocument> newDocsChunk = newDocs.subList(docsStartChunkIndex, endOfArray);

            LOG.warn(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                    "Chunk SOLR update of size {}",newDocsChunk.size());

            updateDocumentsChunk(newDocsChunk, publishingBatchId);

        }

    }
        /**
         * Updates a list of documents with new fields/properties
         *
         * @param newDocs the list of documents and the new fields
         */
    private void updateDocumentsChunk (List<CRFSolrDocument> newDocs,final String publishingBatchId) throws IOException, SolrServerException {
        final Timer.Context context = PublisherMetrics.Publisher.Write.Solr.solrUpdateDocumentsDuration.time(connectionId);
        try
        {
            if (null == newDocs || newDocs.isEmpty()) {

                LOG.warn(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                         "Received for updating and empty/null list. Ignoring");
            }

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                     "Number of Documents trying to update: " + newDocs.size()
                    );

            final List<SolrInputDocument> inputDocuments = new ArrayList<>();

            // Adding individual documents in server
            for (final CRFSolrDocument crfSolrDocument : newDocs) {

                final SolrInputDocument update = new SolrInputDocument();

                update.addField("europeana_id", crfSolrDocument.getRecordId());
                update.addField("has_landingpage", singletonMap("set", crfSolrDocument.getHasLandingPage()));
                update.addField("is_fulltext", singletonMap("set", crfSolrDocument.getIsFullText()));
                update.addField("has_thumbnails", singletonMap("set", crfSolrDocument.getHasThumbnails()));
                update.addField("has_media", singletonMap("set", crfSolrDocument.getHasMedia()));
                update.addField("filter_tags", singletonMap("set", crfSolrDocument.getFilterTags()));
                update.addField("facet_tags", singletonMap("set", crfSolrDocument.getFacetTags()));

                if (null != crfSolrDocument) {
                    update.addField("provider_aggregation_edm_object", crfSolrDocument.getProviderEdmObject());
                }

                inputDocuments.add(update);
            }

            int numberOfDocumentsToUpdate = 0;
            int badDocuments = 0;
            final SolrClient server = createServer(publishingBatchId);
            try {
                LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                         "Trying to do a bulk update on solr"
                        );

                server.add(inputDocuments);
                numberOfDocumentsToUpdate = inputDocuments.size();
            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                          "There was a problem with bulk update. Adding documents one by one and skipping the troubled ones.",
                          e);

                for (final SolrInputDocument document : inputDocuments) {
                    try {
                        server.add(document);
                        ++numberOfDocumentsToUpdate;
                    } catch (Exception e1) {
                        ++badDocuments;
                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                                   publishingBatchId),
                                  "Problems adding document with europeana_id (recordId): {}", document.getField("europeana_id"), e);
                    }

                }
            }
            PublisherMetrics.Publisher.Write.Solr.badSolrDocumentsCount.inc(connectionId, badDocuments);

            for (int retry = 0; retry <= MAX_RETRIES; ++retry) {
                try {
                    LOG.info(LoggingComponent
                                     .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId,
                                                      null, null),
                             "Trying to update {} SOLR documents with commit retry policy. Current retry count is {}",
                             newDocs.size(), retry);
                    if (solrCommitEnabled) {
                        server.commit();
                    } else {
                        LOG.info(LoggingComponent
                                        .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId,
                                                null, null),
                                "Skip committing {} SOLR documents as solrCommitEnabled == false. The SOLR server is responsible to commit by itself.",
                                newDocs.size(), retry);

                    }
                    server.close();
                    PublisherMetrics.Publisher.Write.Solr.totalNumberOfDocumentsWrittenToSolr.inc(numberOfDocumentsToUpdate);
                    PublisherMetrics.Publisher.Write.Solr.totalNumberOfDocumentsWrittenToOneConnection.inc(connectionId, numberOfDocumentsToUpdate);
                }
                catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                              "Failed to update {} SOLR documents with commit retry policy. Current retry count is {}",
                              newDocs.size(), retry);

                    if (retry >= MAX_RETRIES) {
                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                                  "Failed to update {} SOLR documents with commit retry policy. Reached maximum retry" + " count {}. Skipping updating all these documents.",
                                  newDocs.size(), MAX_RETRIES);
                        throw e;
                    }
                    else {
                        try {
                            retry++;
                            final long secsToSleep = retry * 10;
                            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId),
                                      "Failed to update " + newDocs.size() + " SOLR documents with commit retry " +
                                              "policy. Current retry count is " + retry + " . Sleeping " + secsToSleep + " seconds before trying again.");
                            TimeUnit.SECONDS.sleep(secsToSleep);
                        } catch (InterruptedException e1) {
                            throw e;
                        }
                    }
                }
            }
        }
        finally {
            context.close();
        }
    }


    /**
     * Checks existence of documents in the SOLR index.
     *
     * @param documents the document that need to be checked
     * @return the map that indicates for each document id (map key) whether exists : true or false
     */
    public List<HarvesterRecord> filterDocumentIds (final List<HarvesterRecord> documents,final String publishingBatchId) throws IOException, SolrServerException {
        final Timer.Context context = PublisherMetrics.Publisher.Read.Solr.solrCheckIdsDurationDuration.time(connectionId);
        try {
            if (null == documents || documents.isEmpty()) {
                return Collections.EMPTY_LIST;
            }

            final Set<String> acceptedRecordIds = new HashSet<>();
            final List<String> documentIds = new ArrayList<>();

            for (final HarvesterRecord document : documents) {
                documentIds.add(document.getRecordId());
            }

            // As the SOLR query has limitations it cannot handle queries that are too large => we need to break them in parts
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId, null, null),
                     "Checking records: " + documents.size());
            for (int documentIdsStartChunkIndex = 0; documentIdsStartChunkIndex <= documentIds.size();
                 documentIdsStartChunkIndex += MAX_NUMBER_OF_IDS_IN_SOLR_QUERY) {
                final int endOfArray = Math.min(documentIds.size(), documentIdsStartChunkIndex + MAX_NUMBER_OF_IDS_IN_SOLR_QUERY);
                final List<String> documentIdsToQuery = documentIds.subList(documentIdsStartChunkIndex, endOfArray);

                if (!documentIdsToQuery.isEmpty()) {
                    // Do the SOLR query
                    final SolrQuery query = new SolrQuery();

                    final String queryString = "(" + StringUtils.join(documentIdsToQuery, " OR ").replace("/", "\\/") + ")";
                    query.setRows(MAX_NUMBER_OF_IDS_IN_SOLR_QUERY + 1);
                    query.setQuery("europeana_id:" + queryString);

                    try {
                        final SolrClient server = createServer(publishingBatchId);


                        final QueryResponse response = server.query(query, SolrRequest.METHOD.POST);
                        // Mark in the result the documents id's that have been found
                        if (response != null) {
                            final SolrDocumentList solrResults = response.getResults();
                            for (int resultEntryIndex = 0; resultEntryIndex < solrResults.size(); ++resultEntryIndex)
                                acceptedRecordIds.add(solrResults.get(resultEntryIndex).getFieldValue("europeana_id")
                                                                 .toString());
                        }
                        server.close();
                    } catch (Exception e) {

                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                                   publishingBatchId, null, null),
                                  "Failed when executing existence query for {} documents. Will mark the documents as non-existent. The query is {}",
                                  MAX_NUMBER_OF_IDS_IN_SOLR_QUERY, queryString);
                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                                   publishingBatchId, null, null),
                                  "The exception is",
                                  MAX_NUMBER_OF_IDS_IN_SOLR_QUERY, e
                                 );
                        throw e;
                    }
                }
            }

            final List<HarvesterRecord> filteredDocuments = new ArrayList<>();
            for (final HarvesterRecord document: documents) {
               if (acceptedRecordIds.contains(document.getRecordId())) {
                   filteredDocuments.add(document);
               }
            }

            PublisherMetrics.Publisher.Read.Solr.totalNumberOfDocumentsThatExistInSolr.inc(filteredDocuments.size());
            PublisherMetrics.Publisher.Read.Solr.totalNumberOfDocumentsThatExistInOneSolr.inc(connectionId, filteredDocuments.size());

            return filteredDocuments;
        }
        finally {
            context.close();
        }
    }

    public String getConnectionId () {
        return connectionId;
    }
}
