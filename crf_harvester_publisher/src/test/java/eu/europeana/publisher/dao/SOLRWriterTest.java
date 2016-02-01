package eu.europeana.publisher.dao;

import com.mongodb.DBCursor;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.DBTargetConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.extract.FakeTagExtractor;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import utilities.ConfigUtils;
import utilities.TestSolrServer;
import utilities.MongoDatabase;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;


/**
 * Created by salexandru on 09.06.2015.
 */

@Ignore
public class SOLRWriterTest {
    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";
    private static final String SOLR_XML_PATH = "./src/test/resources/solr.xml";

    private PublisherConfig publisherConfig;

    private static final String testBatchId = "tst-batch";
    private SOLRWriter solrWriter;
    private List<HarvesterRecord> harvesterRecords;
    private List<HarvesterRecord> validRecords;
    private MongoDatabase mongoDatabase = null;
    private TestSolrServer testSolrServer = null;

    @Before
    public void setUp() throws IOException {
        publisherConfig = ConfigUtils
                                  .createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");
        mongoDatabase = new MongoDatabase((publisherConfig));
        testSolrServer = new TestSolrServer(publisherConfig.getTargetDBConfig().get(0),SOLR_XML_PATH);

        solrWriter = new SOLRWriter(publisherConfig.getTargetDBConfig().get(0));

        testSolrServer.loadSOLRData(DATA_PATH_PREFIX + "solrData.json", publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "LastSourceDocumentProcessingStatistics");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "sourceDocumentReference.json", "SourceDocumentReference");
        mongoDatabase.loadMongoData(publisherConfig.getTargetDBConfig().get(0).getMongoConfig(), DATA_PATH_PREFIX + "aggregation.json", "Aggregation");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(100, null);

        harvesterRecords = europeanaDao.retrieveRecords(cursor, "");
        validRecords = new ArrayList<>(harvesterRecords);

        int size = harvesterRecords.size();
        for (int i = 0; i < size; ++i) {
            final ReferenceOwner referenceOwner = new ReferenceOwner(UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(), UUID.randomUUID().toString());

            ProcessingJobSubTaskStats subTaskStats = new ProcessingJobSubTaskStats()
                                                             .withRetrieveState(ProcessingJobRetrieveSubTaskState
                                                                                        .SUCCESS)
                                                             .withColorExtractionState(ProcessingJobSubTaskState.SUCCESS)
                                                             .withMetaExtractionState(ProcessingJobSubTaskState.SUCCESS)
                                                             .withThumbnailGenerationState(ProcessingJobSubTaskState.SUCCESS)
                                                             .withThumbnailStorageState(ProcessingJobSubTaskState
                                                                                                .SUCCESS);

            final HarvesterDocument document = new HarvesterDocument(
                    UUID.randomUUID().toString(),
                    DateTime.now(),
                    referenceOwner,
                    new SourceDocumentReferenceMetaInfo("", null, null, null, null),
                    subTaskStats,
                    0 == (i % 2) ? URLSourceType.ISSHOWNBY : URLSourceType.ISSHOWNAT,
                    DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                    "http://www.google.com");
            final HarvesterRecord record = new HarvesterRecord().with(document.getUrlSourceType(), document);
            harvesterRecords.add(record);
        }
    }

    @After
    public void tearDown() {
        mongoDatabase.cleanMongoDatabase();
        testSolrServer.cleanSolrDatabase(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        testSolrServer.shutDown();
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_NullSolrUrl() {
        new SOLRWriter(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void test_EmptySolrUrl() {
        new SOLRWriter(new DBTargetConfig(null, "\t\t\t\n\r",false, "",null,null,10));
    }

    @Test
    public void test_FilterDocuments_NullList() throws SolrServerException, IOException {
        assertTrue(solrWriter.filterDocumentIds(null, testBatchId).isEmpty());
    }

    @Test
    public void test_FilterDocuments_EmptyList() throws SolrServerException, IOException {
        assertTrue(solrWriter.filterDocumentIds(Collections.EMPTY_LIST, testBatchId).isEmpty());
    }

    @Test
    public void test_FilterDocuments_AllValid() throws SolrServerException, IOException {
        assertArrayEquals(validRecords.toArray(), solrWriter.filterDocumentIds(validRecords, testBatchId).toArray());
    }

    @Test
    public void test_FilterDocuments_SomeAreInvalid() throws SolrServerException, IOException {
        assertArrayEquals(validRecords.toArray(),
                          solrWriter.filterDocumentIds(harvesterRecords, testBatchId).toArray());
    }

    @Test
    public void test_FilterDocuments_AllInvalid() throws SolrServerException, IOException {
        final List<HarvesterRecord> invalidRecords = harvesterRecords.subList(10, harvesterRecords.size());
        solrWriter.filterDocumentIds(invalidRecords, testBatchId).isEmpty();
    }


    @Test
    public void test_UpdateDocuments_NullList() throws IOException, SolrServerException {
        solrWriter.updateDocuments(null, testBatchId);
    }

    @Test
    public void test_UpdateDocuments_EmptyList() throws IOException, SolrServerException {
        solrWriter.updateDocuments(Collections.EMPTY_LIST, testBatchId);
    }

    @Test
    public void test_UpdateDocuments() throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();


        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validRecords, testBatchId), testBatchId);


        query.clear();
        query.setQuery("is_fulltext:*");
        query.addField("europeana_id");

        try {
            assertEquals(validRecords.size(),
                         solrServer.query(query).getResults().size()
                        );
        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }

    @Test
    public void test_UpdateDocuments_UrlSourceBy() throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();


        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validRecords, testBatchId), testBatchId);


        query.clear();
        query.addField("provider_aggregation_edm_object");
        query.addField("europeana_id");

        try {

            for (final HarvesterRecord record: validRecords) {
                for (final HarvesterDocument document: record.getAllDocuments()) {
                    query.setQuery("europeana_id:\"" + document.getReferenceOwner().getRecordId() + "\"");
                    if (URLSourceType.ISSHOWNBY == document.getUrlSourceType() &&
                        ProcessingJobSubTaskState.SUCCESS.equals(document.getSubTaskStats()
                                                                         .getThumbnailGenerationState()) &&
                        ProcessingJobSubTaskState.SUCCESS.equals(document.getSubTaskStats().getThumbnailStorageState())) {
                        assertEquals(Arrays.asList(document.getUrl()).toString(),
                                     solrServer.query(query).getResults().get(0).get("provider_aggregation_edm_object")
                                               .toString());
                    }
                }
            }

        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }


    @Test
    public void test_UpdateDocuments_HasLandingPage() throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getTargetDBConfig().get(0).getSolrUrl());
        final SolrQuery query = new SolrQuery();


        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validRecords, testBatchId), testBatchId);


        query.clear();
        query.addField("has_landingpage");
        query.addField("europeana_id");

        try {

            for (final HarvesterRecord record: validRecords) {
                for (final HarvesterDocument document : record.getAllDocuments()) {
                    query.setQuery("europeana_id:\"" + document.getReferenceOwner().getRecordId() + "\"");
                    if (URLSourceType.ISSHOWNAT == document.getUrlSourceType()) {
                        assertEquals(ProcessingJobRetrieveSubTaskState.SUCCESS
                                             .equals(document.getSubTaskStats().getRetrieveState()),
                                     solrServer.query(query).getResults().get(0).get("has_landingpage"));
                    }
                }
            }

        } catch (SolrServerException e) {
            fail("Solr Query Failed: " + e.getMessage() + "\n" + Arrays.deepToString(e.getStackTrace()));
        }
    }
}
