package eu.europeana.publisher.dao;

import com.mongodb.DBCursor;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.extract.FakeTagExtractor;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;
import utilities.ConfigUtils;
import utilities.DButils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.*;
import static utilities.DButils.loadMongoData;

/**
 * Created by salexandru on 09.06.2015.
 */
public class SOLRWriterTest {
    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private PublisherConfig publisherConfig;

    private static final String testBatchId = "tst-batch";
    private SOLRWriter solrWriter;
    private List<HarvesterDocument> harvesterDocuments;
    private List<HarvesterDocument> validDocuments;


    @Before
    public void setUp() throws IOException {
        publisherConfig = ConfigUtils
                                  .createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");
        solrWriter = new SOLRWriter(publisherConfig.getTargetDBConfig().get(0));

        DButils.loadSOLRData(DATA_PATH_PREFIX + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json",
                      "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json",
                "SourceDocumentReferenceMetaInfo");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "sourceDocumentReference.json", "SourceDocumentReference");
        loadMongoData(publisherConfig.getTargetDBConfig().get(0).getMongoConfig(), DATA_PATH_PREFIX + "aggregation.json", "Aggregation");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        harvesterDocuments = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, cursor.count());
        validDocuments = new ArrayList<>(harvesterDocuments);

        int size = harvesterDocuments.size();
        for (int i = 0; i < size; ++i) {
            final ReferenceOwner referenceOwner = new ReferenceOwner(UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString());

            ProcessingJobSubTaskStats subTaskStats = new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobRetrieveSubTaskState.SUCCESS)
                                                                              .withColorExtractionState(ProcessingJobSubTaskState.SUCCESS)
                                                                              .withMetaExtractionState(ProcessingJobSubTaskState.SUCCESS)
                                                                              .withThumbnailGenerationState(ProcessingJobSubTaskState.SUCCESS)
                                                                              .withThumbnailStorageState(ProcessingJobSubTaskState.SUCCESS);

            harvesterDocuments.add(new HarvesterDocument(UUID.randomUUID().toString(), DateTime.now(),
                                                         referenceOwner,
                                                         new SourceDocumentReferenceMetaInfo("", null, null, null, null),
                                                         subTaskStats,
                                                         0 == i % 2 ? URLSourceType.ISSHOWNBY: URLSourceType.ISSHOWNAT,
                                                         DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                                         "http://www.google.com"
                                                        )
            );
        }
    }

    @After
    public void tearDown() {
        DButils.cleanMongoDatabase(publisherConfig);
    //    DButils.cleanSolrDatabase(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_NullSolrUrl() {
        new SOLRWriter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EmptySolrUrl() {
        new SOLRWriter(new DBTargetConfig(null, "\t\t\t\n\r", ""));
    }

    @Test
    public void test_FilterDocuments_NullList() throws SolrServerException {
        assertTrue(solrWriter.filterDocumentIds(null, testBatchId).isEmpty());
    }

    @Test
    public void test_FilterDocuments_EmptyList() throws SolrServerException {
        assertTrue(solrWriter.filterDocumentIds(Collections.EMPTY_LIST, testBatchId).isEmpty());
    }

    @Test
    public void test_FilterDocuments_AllValid() throws SolrServerException {
        assertArrayEquals(validDocuments.toArray(), solrWriter.filterDocumentIds(validDocuments, testBatchId).toArray());
    }

    @Test
    public void test_FilterDocuments_SomeAreInvalid() throws SolrServerException {
        assertArrayEquals(validDocuments.toArray(),
                          solrWriter.filterDocumentIds(harvesterDocuments, testBatchId).toArray());
    }

    @Test
    public void test_FilterDocuments_AllInvalid() throws SolrServerException {
        final List<HarvesterDocument> invalidDocuments = harvesterDocuments.subList(6, harvesterDocuments.size());
        assertTrue(solrWriter.filterDocumentIds(invalidDocuments, testBatchId).isEmpty());
    }


    @Test
    public void test_UpdateDocuments_NullList() throws IOException, SolrServerException {
        assertFalse(solrWriter.updateDocuments(null, testBatchId));
    }

    @Test
    public void test_UpdateDocuments_EmptyList() throws IOException, SolrServerException {
        assertFalse(solrWriter.updateDocuments(Collections.EMPTY_LIST, testBatchId));
    }

    @Test
    public void test_UpdateDocuments() throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();


        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validDocuments, testBatchId), testBatchId);


        query.clear();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(validDocuments.size(), solrServer.query(query).getResults().size());
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void test_UpdateDocuments_UrlSourceBy() throws IOException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();


        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validDocuments, testBatchId), testBatchId);


        query.clear();
        query.addField("provider_aggregation_edm_object");
        query.addField("europeana_id");

        try {

            for (final HarvesterDocument document: validDocuments) {
                query.setQuery("europeana_id:\"" + document.getReferenceOwner().getRecordId() + "\"");
                if (URLSourceType.ISSHOWNBY == document.getUrlSourceType()) {
                    assertEquals(Arrays.asList(document.getUrl()).toString(),
                                 solrServer.query(query).getResults().get(0).get("provider_aggregation_edm_object").toString());
                }
            }

        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    @Ignore
    public void test_UpdateDocuments_PreserveFields_SimpleData() throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();

        query.setQuery("*:*");
        query.addField("RIGHTS");
        query.addField("TYPE");
        query.addField("LANGUAGE");
        query.addField("PROVIDER");
        query.addField("europeana_id");
        query.addSort("europeana_id", SolrQuery.ORDER.asc);

        final SolrDocumentList resultsBeforeUpdate = solrServer.query(query).getResults();

        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validDocuments, testBatchId), testBatchId);

        final SolrDocumentList resultsAfterUpdate = solrServer.query(query).getResults();

        assertEquals(resultsBeforeUpdate.getNumFound(), resultsAfterUpdate.getNumFound());

        Iterator<SolrDocument> beforeIter = resultsBeforeUpdate.listIterator();
        Iterator<SolrDocument> afterIter = resultsAfterUpdate.listIterator();

        while (beforeIter.hasNext() && afterIter.hasNext()) {
            ReflectionAssert.assertReflectionEquals(beforeIter.next(), afterIter.next());
        }
    }

    @Test
    @Ignore
    public void test_UpdateDocuments_PreserveFields_ProblamaticData() throws IOException, SolrServerException {
        tearDown();
        setUpSolrSpecialCase();


        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();

        query.setQuery("*:*");
        query.addField("RIGHTS");
        query.addField("TYPE");
        query.addField("LANGUAGE");
        query.addField("PROVIDER");
        query.addField("europeana_id");
        query.addSort("europeana_id", SolrQuery.ORDER.asc);

        final SolrDocumentList resultsBeforeUpdate = solrServer.query(query).getResults();

        //       solrWriter.updateDocuments(FakeTagExtractor.extractTags(validDocuments));

        final SolrDocumentList resultsAfterUpdate = solrServer.query(query).getResults();

        assertEquals(resultsBeforeUpdate.getNumFound(), resultsAfterUpdate.getNumFound());

        Iterator<SolrDocument> beforeIter = resultsBeforeUpdate.listIterator();
        Iterator<SolrDocument> afterIter = resultsAfterUpdate.listIterator();

        while (beforeIter.hasNext() && afterIter.hasNext()) {
            ReflectionAssert.assertReflectionEquals(beforeIter.next(), afterIter.next());
        }
    }

    private void setUpSolrSpecialCase() throws UnknownHostException {
        solrWriter = new SOLRWriter(publisherConfig.getTargetDBConfig().get(0));

        DButils.loadSOLRData(DATA_PATH_PREFIX + "solrSpecialCase/solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "solrSpecialCase/jobStatistics.json",
                "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "solrSpecialCase/metaInfo.json",
                "SourceDocumentReferenceMetaInfo");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        harvesterDocuments = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, cursor.count());
        validDocuments = new ArrayList<>(harvesterDocuments);

        int size = harvesterDocuments.size();
        for (int i = 0; i < size; ++i) {
            final ReferenceOwner referenceOwner = new ReferenceOwner(UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString());

            ProcessingJobSubTaskStats subTaskStats = new ProcessingJobSubTaskStats().withRetrieveState(ProcessingJobRetrieveSubTaskState.SUCCESS)
                                                                              .withColorExtractionState(ProcessingJobSubTaskState.SUCCESS)
                                                                              .withMetaExtractionState(ProcessingJobSubTaskState.SUCCESS)
                                                                              .withThumbnailGenerationState(ProcessingJobSubTaskState.SUCCESS)
                                                                              .withThumbnailStorageState(ProcessingJobSubTaskState.SUCCESS);

            harvesterDocuments.add(new HarvesterDocument(UUID.randomUUID().toString(), DateTime.now(),
                                                         referenceOwner,
                                                         new SourceDocumentReferenceMetaInfo("", null, null, null, null),
                                                         subTaskStats,
                                                         0 == i % 2 ? URLSourceType.ISSHOWNBY: URLSourceType.ISSHOWNAT,
                                                         DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD,
                                                         "http://www.google.com"
                                   )
                                  );
        }
    }
}
