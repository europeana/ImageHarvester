package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utilities.ConfigUtils;
import utilities.DButils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.unitils.reflectionassert.ReflectionAssert;

import static org.junit.Assert.*;
import static utilities.DButils.loadMongoData;

/**
 * Created by salexandru on 09.06.2015.
 */
public class PublisherEuropeanaDaoTest {
    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private  PublisherConfig publisherConfig;

    private PublisherEuropeanaDao europeanaDao;


    @Before
    public void setUp() throws IOException {
        publisherConfig = ConfigUtils.createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");
        europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());

        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json",
                      "SourceDocumentReferenceMetaInfo");
    }

    @After
    public void tearDown() {
        DButils.cleanMongoDatabase(publisherConfig);
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_NullConfig () throws UnknownHostException {
       new PublisherEuropeanaDao(null);
    }

    @Test
    public void test_buildCursor_withoutDataFilter() {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        assertEquals (new BasicDBObject(), cursor.getQuery());
    }

    @Test
    public void test_buildCursor_withDataFilter() {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(publisherConfig.getStartTimestamp());
        final BasicDBObject query = new BasicDBObject();

        query.put ("updatedAt", new BasicDBObject("$gt", publisherConfig.getStartTimestamp().toDate()));

        assertEquals(query, cursor.getQuery());
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_extractDocuments_NullCursor () {
        europeanaDao.retrieveDocumentsWithMetaInfo(null, 10);
    }

    @Test (expected =  IllegalArgumentException.class)
    public void test_extractDocuments_NegativeBatchSize() {
        europeanaDao.retrieveDocumentsWithMetaInfo(null, -1);
    }

    @Test
    public void test_extractDocuments_batchSizeOne () {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        checkDocuments(cursor, 1, null);
    }

    @Test
    public void test_extractDocuments_batchSizeTwo () {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        checkDocuments(cursor, 2, null);
    }

    @Test
    public void test_extractDocument_withDataFilter_batchSizeTwo() {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(publisherConfig.getStartTimestamp());

        checkDocuments(cursor, 2, publisherConfig.getStartTimestamp());
    }

    @Test
    public void test_extractMetaInfo_emptyListIds() {
       assertTrue(europeanaDao.retrieveMetaInfo(Collections.EMPTY_LIST).isEmpty());
    }

    @Test
    public void test_extractMetaInfo_nullListIds() {
        assertTrue(europeanaDao.retrieveMetaInfo(null).isEmpty());
    }

    @Test
    public void test_extracMetaInfo() {
        final Datastore datastore = new Morphia().createDatastore(publisherConfig.getSourceMongoConfig().connectToMongo(),
                                                                  publisherConfig.getSourceMongoConfig().getDbName()
                                                                 );

        final SourceDocumentReferenceMetaInfoDao metaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);

        final List<String> ids = Arrays.asList("902b31943b4e66f5578539bfa60f2b82", "75a47a2953516381bd9d4d1220bdcfc3",
                                               "46787456ff68f404835064723a4d1cd9", "cc6103bee8aa27bfa11b851a2377a015",
                                               "764fff8f4654274f9cff28280d7e0008", "2d74ba5b07344f9587b5c67693fcb3a5");

        final List<SourceDocumentReferenceMetaInfo> metaInfos = europeanaDao.retrieveMetaInfo(ids);

        assertEquals(ids.size(), metaInfos.size());

        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final SourceDocumentReferenceMetaInfo correctMetaInfo = metaInfoDao.read(metaInfo.getId());

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, metaInfo);

        }

    }


    private void checkDocuments (final DBCursor cursor, final int batchSize, final DateTime filter) {
        final DBCollection jobStatistics = publisherConfig.getSourceMongoConfig().connectToDB().getCollection("SourceDocumentProcessingStatistics");
        final DBCollection metaInfos = publisherConfig.getSourceMongoConfig().connectToDB().getCollection("SourceDocumentReferenceMetaInfo");
        while (cursor.hasNext()) {
            final List<HarvesterDocument> documents = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, batchSize);

            assertEquals(batchSize, documents.size());

            for (final HarvesterDocument document : documents) {
                if (null != filter) {
                    assertTrue (filter.isBefore(document.getUpdatedAt()));
                }

                {
                    final BasicDBObject queryFindDocument = new BasicDBObject();


                    queryFindDocument.put("sourceDocumentReferenceId",
                                          document.getSourceDocumentReferenceId());
                    queryFindDocument.put("updatedAt", document.getUpdatedAt().toDate());
                    queryFindDocument.put("referenceOwner.recordId", document.getReferenceOwner().getRecordId());

                    assertEquals(1L, jobStatistics.count(queryFindDocument));
                }

                {
                    final BasicDBObject queryFindMetaInfo = new BasicDBObject();

                    queryFindMetaInfo.put ("_id", document.getSourceDocumentReferenceId());

                    assertEquals(1L, metaInfos.count(queryFindMetaInfo));
                }
            }
        }
    }
}
