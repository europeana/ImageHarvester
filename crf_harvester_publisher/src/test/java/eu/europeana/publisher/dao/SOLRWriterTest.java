package eu.europeana.publisher.dao;

import com.mongodb.DBCursor;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.publisher.domain.DocumentStatistic;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.domain.RetrievedDocument;
import eu.europeana.publisher.logic.extractor.FakeTagExtractor;
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
import static org.junit.Assert.fail;
import static utilities.DButils.loadMongoData;

/**
 * Created by salexandru on 09.06.2015.
 */
public class SOLRWriterTest {
    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private static final PublisherConfig publisherConfig = ConfigUtils
                                                                   .createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");

    private SOLRWriter solrWriter;
    private List<RetrievedDocument> retrievedDocuments;
    private List<RetrievedDocument> validDocuments;


    @Before
    public void setUp () throws UnknownHostException {
        solrWriter = new SOLRWriter(publisherConfig.getSolrURL());

        DButils.loadSOLRData(DATA_PATH_PREFIX + "solrData.json", publisherConfig.getSolrURL());
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json",
                      "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json",
                      "SourceDocumentReferenceMetaInfo");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        retrievedDocuments = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, cursor.count());
        validDocuments = new ArrayList<>(retrievedDocuments);

        int size = retrievedDocuments.size();
        for (int i = 0; i < size; ++i) {
            retrievedDocuments.add(new RetrievedDocument(new DocumentStatistic(UUID.randomUUID().toString(),
                                                                               UUID.randomUUID().toString(), DateTime.now()),
                                                         new SourceDocumentReferenceMetaInfo("", null, null, null,
                                                                                             null)));
        }
    }

    @After
    public void tearDown () {
        DButils.cleanMongoDatabase(publisherConfig.getSourceMongoConfig(), publisherConfig.getTargetMongoConfig());
       // DButils.cleanSolrDatabase(publisherConfig.getSolrURL());
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_NullSolrUrl () {
        new SOLRWriter(null);
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_EmptySolrUrl () {
        new SOLRWriter("\t\t\n\r");
    }

    @Test
    public void test_FilterDocuments_NullList () throws SolrServerException {
        assertTrue(solrWriter.filterDocumentIds(null).isEmpty());
    }

    @Test
    public void test_FilterDocuments_EmptyList () throws SolrServerException {
        assertTrue(solrWriter.filterDocumentIds(Collections.EMPTY_LIST).isEmpty());
    }

    @Test
    public void test_FilterDocuments_AllValid () throws SolrServerException {
        assertArrayEquals(validDocuments.toArray(), solrWriter.filterDocumentIds(validDocuments).toArray());
    }

    @Test
    public void test_FilterDocuments_SomeAreInvalid () throws SolrServerException {
        assertArrayEquals(validDocuments.toArray(), solrWriter.filterDocumentIds(retrievedDocuments).toArray());
    }

    @Test
    public void test_FilterDocuments_AllInvalid () throws SolrServerException {
        final List<RetrievedDocument> invalidDocuments = retrievedDocuments.subList(6, retrievedDocuments.size());
        assertTrue(solrWriter.filterDocumentIds(invalidDocuments).isEmpty());
    }


    @Test
    public void test_UpdateDocuments_NullList () throws IOException, SolrServerException {
        assertFalse(solrWriter.updateDocuments(null));
    }

    @Test
    public void test_UpdateDocuments_EmptyList () throws IOException, SolrServerException {
        assertFalse(solrWriter.updateDocuments(Collections.EMPTY_LIST));
    }

    @Test
    public void test_UpdateDocuments () throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();


        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validDocuments));


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
    @Ignore
    public void test_UpdateDocuments_PreserveFields_SimpleData() throws IOException, SolrServerException {
        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getSolrURL());
        final SolrQuery query = new SolrQuery();

        query.setQuery("*:*");
        query.addField("RIGHTS");
        query.addField("TYPE");
        query.addField("LANGUAGE");
        query.addField("PROVIDER");
        query.addField("europeana_id");
        query.addSort("europeana_id", SolrQuery.ORDER.asc);

        final SolrDocumentList resultsBeforeUpdate = solrServer.query(query).getResults();

        solrWriter.updateDocuments(FakeTagExtractor.extractTags(validDocuments));

        final SolrDocumentList resultsAfterUpdate = solrServer.query(query).getResults();

        assertEquals (resultsBeforeUpdate.getNumFound(), resultsAfterUpdate.getNumFound());

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


        final HttpSolrClient solrServer = new HttpSolrClient(publisherConfig.getSolrURL());
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

        assertEquals (resultsBeforeUpdate.getNumFound(), resultsAfterUpdate.getNumFound());

        Iterator<SolrDocument> beforeIter = resultsBeforeUpdate.listIterator();
        Iterator<SolrDocument> afterIter = resultsAfterUpdate.listIterator();

        while (beforeIter.hasNext() && afterIter.hasNext()) {
            ReflectionAssert.assertReflectionEquals(beforeIter.next(), afterIter.next());
        }
    }

    private void setUpSolrSpecialCase () throws UnknownHostException {
        solrWriter = new SOLRWriter(publisherConfig.getSolrURL());

        DButils.loadSOLRData(DATA_PATH_PREFIX + "solrSpecialCase/solrData.json", publisherConfig.getSolrURL());
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "solrSpecialCase/jobStatistics.json",
                      "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "solrSpecialCase/metaInfo.json",
                      "SourceDocumentReferenceMetaInfo");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        retrievedDocuments = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, cursor.count());
        validDocuments = new ArrayList<>(retrievedDocuments);

        int size = retrievedDocuments.size();
        for (int i = 0; i < size; ++i) {
            retrievedDocuments.add(new RetrievedDocument(new DocumentStatistic(UUID.randomUUID().toString(),
                                                                               UUID.randomUUID().toString(), DateTime.now()),
                                                         new SourceDocumentReferenceMetaInfo("", null, null, null,
                                                                                             null)));
        }
    }
}
