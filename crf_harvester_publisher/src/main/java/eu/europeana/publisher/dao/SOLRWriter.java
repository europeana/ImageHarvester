package eu.europeana.publisher.dao;

import com.codahale.metrics.Timer;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.PublisherMetrics;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.BinaryResponseParser;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;
import org.slf4j.LoggerFactory;

import javax.swing.text.Document;
import java.io.IOException;
import java.nio.charset.Charset;
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

    /*
     *  Connection timeout for solr queries. Time unit is milliseconds
     */
    private static final int CONNECTION_TIMEOUT = 5000;

    private static final int MAX_RETRIES = 5;
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
    private final String solrUrl;
    private final String connectionId;


    public SOLRWriter (final DBTargetConfig solrConfig) {
        if (null == solrConfig || null == solrConfig.getSolrUrl() || solrConfig.getSolrUrl().trim().isEmpty()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR),
                    "Cannot initialise the SOLR persistence as the provided Sorl base url is null.Exiting.");
            throw new IllegalArgumentException("Solr Url cannot be null");
        }

        this.connectionId = solrConfig.getName();
        this.solrUrl = solrConfig.getSolrUrl();
    }

    private SolrClient createServer (String publishingBatchId) {
        int retry = 0;
        while (true) {
            try {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                           publishingBatchId, null, null),
                          "Trying to connect to the solr server: " + solrUrl + ". Retry #" + retry);
                final RequestConfig.Builder requestBuilder = RequestConfig.custom()
                                                                          .setConnectTimeout(CONNECTION_TIMEOUT)
                                                                          .setConnectionRequestTimeout(CONNECTION_TIMEOUT);
                final HttpClientBuilder clientBuilder = HttpClientBuilder.create()
                                                                         .setDefaultRequestConfig(requestBuilder
                                                                                                          .build());
                final HttpSolrClient server = new HttpSolrClient(solrUrl, clientBuilder.build(), new BinaryResponseParser());
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
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

    /**
     * Updates a list of documents with new fields/properties
     *
     * @param newDocs the list of documents and the new fields
     */
    public boolean updateDocuments (List<CRFSolrDocument> newDocs,final String publishingBatchId) throws IOException{
        final Timer.Context context = PublisherMetrics.Publisher.Write.Solr.solrUpdateDocumentsDuration.time(connectionId);
        try {
            if (null == newDocs || newDocs.isEmpty()) {

                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                           publishingBatchId, null, null),
                          "Received for updating and empty/null list. Ignoring");
                return true;
        }

            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                       publishingBatchId, null, null),
                                                       "Number of Documents trying to update: " + newDocs.size());
            int retry = 0;
            while (retry <= MAX_RETRIES) {
                final SolrClient server = createServer(publishingBatchId);

                int numberOfDocumentsToUpdate = 0;
                // Adding individual documents in server
                for (final CRFSolrDocument CRFSolrDocument : newDocs) {

                    final SolrInputDocument update = new SolrInputDocument();

                    update.addField("europeana_id", CRFSolrDocument.getRecordId());

                    if (DocumentReferenceTaskType.CHECK_LINK == CRFSolrDocument.getTaskType()) {
                        update.addField("has_landingpage", singletonMap("set", CRFSolrDocument.getHasLandingPage()));
                    }
                    else {

                        update.addField("is_fulltext", singletonMap("set", CRFSolrDocument.getIsFulltext()));

                        update.addField("has_thumbnails", singletonMap("set", CRFSolrDocument.getHasThumbnails()));

                        update.addField("has_media", singletonMap("set", CRFSolrDocument.getHasMedia()));

                        update.addField("filter_tags", singletonMap("set", CRFSolrDocument.getFilterTags()));

                        update.addField("facet_tags", singletonMap("set", CRFSolrDocument.getFacetTags()));

                        if (updateEdmObjectUrl(CRFSolrDocument, publishingBatchId)) {
                            update.addField("provider_aggregation_edm_object", CRFSolrDocument.getUrl());
                        }
                    }

                    try {
                        server.add(update);
                        ++numberOfDocumentsToUpdate;
                    } catch (Exception e) {
                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                                   publishingBatchId, null,
                                                                   new ReferenceOwner(null, null, CRFSolrDocument
                                                                                                          .getRecordId())),
                                  "Exception when adding specific document " + update.toString() + " => document " +
                                          "skipped",
                                  e);
                    }
                }

                try {
                    LOG.info(LoggingComponent
                                     .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId,
                                                      null, null),
                             "Trying to update {} SOLR documents with commit retry policy. Current retry count is {}",
                             newDocs.size(), retry);
                    server.commit();
                    server.close();
                    PublisherMetrics.Publisher.Write.Solr.totalNumberOfDocumentsWrittenToSolr.inc(numberOfDocumentsToUpdate);
                    PublisherMetrics.Publisher.Write.Solr.totalNumberOfDocumentsWrittenToOneConnection.inc(connectionId,
                                                                                                           numberOfDocumentsToUpdate);
                    return true;
                }
                catch (Exception e) {
                    LOG.error(LoggingComponent
                                      .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId,
                                                       null, null),
                              "Failed to update {} SOLR documents with commit retry policy. Current retry count is {}",
                              newDocs.size(), retry);
                    server.close();
                    if (retry >= MAX_RETRIES) {
                        LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                                   publishingBatchId, null, null),
                                  "Failed to update {} SOLR documents with commit retry policy. Reached maximum retry" +
                                          " count {}. Skipping updating all these documents.",
                                  newDocs.size(), MAX_RETRIES);
                        return false;
                    }
                    else {
                        try {
                            retry++;
                            final long secsToSleep = retry * 10;
                            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                                       publishingBatchId, null, null),
                                      "Failed to update " + newDocs.size() + " SOLR documents with commit retry " +
                                              "policy. Current retry count is " + retry + " . Sleeping " + secsToSleep + " seconds before trying again.");
                            TimeUnit.SECONDS.sleep(secsToSleep);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
            return false;
        }
        finally {
            context.close();
        }
    }

    private boolean updateEdmObjectUrl (CRFSolrDocument crfSolrDocument, String publishingBatchId) {
        if (null == crfSolrDocument || null == crfSolrDocument.getUrlSourceType() || null == crfSolrDocument.getUrl()) {
            LOG.error(LoggingComponent
                              .appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR, publishingBatchId, null,
                                               null), "Document with record Id : " + crfSolrDocument.getRecordId() +
                                                                                    "has null url and/or " +
                                                                                    "urlSourceType");
            return false;
        }

        return URLSourceType.ISSHOWNBY.equals(crfSolrDocument.getUrlSourceType()) &&
               ProcessingJobSubTaskState.SUCCESS.equals(crfSolrDocument.getSubTaskStats().getThumbnailGenerationState()) &&
               ProcessingJobSubTaskState.SUCCESS.equals(crfSolrDocument.getSubTaskStats().getThumbnailStorageState());
    }

    /**
     * Checks existence of documents in the SOLR index.
     *
     * @param documents the document that need to be checked
     * @return the map that indicates for each document id (map key) whether exists : true or false
     */
    public List<HarvesterDocument> filterDocumentIds (final List<HarvesterDocument> documents,final String publishingBatchId) {
        final Timer.Context context = PublisherMetrics.Publisher.Read.Solr.solrCheckIdsDurationDuration.time(connectionId);
        try {
            if (null == documents || documents.isEmpty()) {
                return Collections.EMPTY_LIST;
            }


            final Set<String> acceptedRecordIds = new HashSet<>();
            final List<String> documentIds = new ArrayList<>();

            for (final HarvesterDocument document : documents) {
                documentIds.add(document.getReferenceOwner().getRecordId());
            }

            // As the SOLR query has limitations it cannot handle queries that are too large => we need to break them in parts
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_SOLR,
                                                       publishingBatchId, null, null),
                      "Checking documents: " + documents.size());
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
                    }
                }
            }


            final List<HarvesterDocument> filteredDocuments = new ArrayList<>();
            for (final HarvesterDocument document: documents) {
               if (acceptedRecordIds.contains(document.getReferenceOwner().getRecordId())) {
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
