package eu.europeana.publisher.logic;

import eu.europeana.publisher.domain.CRFSolrDocument;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BinaryRequestWriter;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.CommonParams;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;

/**
 * Writes additional fields/properties to SOLR
 */
public class SOLRWriter {

    /**
     * The maximum number of ID's that can be present in a SOLR search query.
     * Important because of the limitations of the HTTP URL length.
     */
    private static final int MAX_NUMBER_OF_IDS_IN_SOLR_QUERY = 100;

    private static final Logger LOG = LogManager.getLogger(SOLRWriter.class.getName());

    private final HttpSolrServer server;

    public SOLRWriter(String url) {
        LOG.info("SOLR writer");
        this.server = new HttpSolrServer(url);
        server.setRequestWriter(new BinaryRequestWriter());
    }

    /**
     * Updates a list of documents with new fields/properties
     *
     * @param newDocs the list of documents and the new fields
     */
    public void updateDocuments(List<CRFSolrDocument> newDocs) throws IOException, SolrServerException {
        for (final CRFSolrDocument CRFSolrDocument : newDocs) {

            final SolrInputDocument update = new SolrInputDocument();

            update.addField("europeana_id", CRFSolrDocument.getRecordId());

            update.addField("is_fulltext", singletonMap("set", CRFSolrDocument.getIsFulltext()));

            update.addField("has_thumbnails", singletonMap("set", CRFSolrDocument.getHasThumbnails()));

            update.addField("has_media", singletonMap("set", CRFSolrDocument.getHasMedia()));

            update.addField("filter_tags", singletonMap("set", CRFSolrDocument.getFilterTags()));

            update.addField("facet_tags", singletonMap("set", CRFSolrDocument.getFacetTags()));

            try {
                server.add(update);
            } catch (Exception e) {
                LOG.error("Solr exception when adding document", e);
                LOG.error("Solr document that caused exception" + update);
                throw e;
            }
        }

        LOG.info("SOLR: committing " + newDocs.size() + " documents");
        server.commit();
    }

    /**
     * Checks existence of documents in the SOLR index.
     *
     * @param documentIds the document id's that need to be checked
     * @return the map that indicates for each document id (map key) whether exists : true or false
     * @throws SolrServerException
     */
    public Map<String, Boolean> documentExists(final List<String> documentIds) throws SolrServerException {

        // Initialise the result : no documents exist
        final Map<String, Boolean> result = new HashMap<String, Boolean>();
        if (documentIds.isEmpty()) {
            return result;
        }

        for (final String id : documentIds) result.put(id, false);

        // As the SOLR query has limitations it cannot handle queries that are too large => we need to break them in parts
        for (int documentIdsStartChunkIndex = 0; documentIdsStartChunkIndex <= documentIds.size(); documentIdsStartChunkIndex += MAX_NUMBER_OF_IDS_IN_SOLR_QUERY) {
            final int endOfArray = (documentIdsStartChunkIndex + MAX_NUMBER_OF_IDS_IN_SOLR_QUERY >= documentIds.size()) ? documentIds.size() : documentIdsStartChunkIndex + MAX_NUMBER_OF_IDS_IN_SOLR_QUERY;
            List<String> documentIdsToQuery = documentIds.subList(documentIdsStartChunkIndex, endOfArray);

            // Do the SOLR query
            final SolrQuery query = new SolrQuery();
            final String queryString = "(" + StringUtils.join(documentIdsToQuery, " OR ").replace("/", "\\/") + ")";
            query.set(CommonParams.Q, "*:*");
            query.set(CommonParams.ROWS, MAX_NUMBER_OF_IDS_IN_SOLR_QUERY + 1);
            query.set(CommonParams.FQ, "europeana_id:" + queryString);
            query.set(CommonParams.FL, "europeana_id");

            try {
                final QueryResponse response = server.query(query);
                // Mark in the result the documents id's that have been found
                if (response != null) {
                    final SolrDocumentList solrResults = response.getResults();
                    for (int resultEntryIndex = 0; resultEntryIndex < solrResults.size(); ++resultEntryIndex)
                        result.put(solrResults.get(resultEntryIndex).getFieldValue("europeana_id").toString(), true);
                }
            } catch (Exception e) {
                LOG.error("SOLR query failed when executing query " + queryString);
                throw e;
            }
        }


        return result;

    }
}
