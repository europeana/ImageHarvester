package eu.europeana.publisher.dao;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.domain.PublisherConfig;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;
import utilities.ConfigUtils;
import utilities.MongoDatabase;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by salexandru on 09.06.2015.
 */
public class PublisherEuropeanaDaoTest {

//    private static final String DATA_PATH_PREFIX = "/Users/paul/Documents/workspace/ImageHarvester/crf_harvester_publisher/src/test/resources/data-files/";
//    private static final String CONFIG_PATH_PREFIX = "/Users/paul/Documents/workspace/ImageHarvester/crf_harvester_publisher/src/test/resources/config-files/";

    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private  PublisherConfig publisherConfig;

    private PublisherEuropeanaDao europeanaDao;

    private static final BasicDBObject orQuery = new BasicDBObject();
    private MongoDatabase mongoDatabase = null;

    static {
        final BasicDBList orList = new BasicDBList();
        orList.add(new BasicDBObject("state", ProcessingState.ERROR.name()));
        orList.add(new BasicDBObject("state", ProcessingState.FAILED.name()));
        orList.add(new BasicDBObject("state", ProcessingState.SUCCESS.name()));
        orQuery.put("$or", orList);
    }


    @Before
    public void setUp() throws IOException {
        publisherConfig = ConfigUtils.createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");
        mongoDatabase = new MongoDatabase(publisherConfig);

        europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());

        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "LastSourceDocumentProcessingStatistics");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "sourceDocumentReference.json", "SourceDocumentReference");
    }

    @After
    public void tearDown() {
        mongoDatabase.cleanMongoDatabase();
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_NullConfig () throws UnknownHostException {
       new PublisherEuropeanaDao(null);
    }

    @Test
    public void test_buildCursor_withoutDataFilter() {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(100, null);

        assertEquals (orQuery, cursor.getQuery());
    }

    @Test
    public void test_buildCursor_withDataFilter() {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(100, publisherConfig.getStartTimestamp());
        final BasicDBObject query = new BasicDBObject();

        query.put ("updatedAt", new BasicDBObject("$gt", publisherConfig.getStartTimestamp().toDate()));
        query.put ("$or", orQuery.get("$or"));

        assertEquals(query, cursor.getQuery());
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_extractDocuments_NullCursor () {
        europeanaDao.retrieveRecords(null, "");
    }

    @Test (expected =  IllegalArgumentException.class)
    public void test_extractDocuments_NegativeBatchSize() {
        europeanaDao.retrieveRecords(null, "");
    }

    @Test
    public void test_extractDocuments_batchSizeOne () {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(1, null);

        checkDocuments(cursor, 1, null);
    }

    @Test
    public void test_extractDocuments_batchSizeTwo () {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(2, null);

        checkDocuments(cursor, 2, null);
    }

    @Test
    public void test_extractDocument_withDataFilter_batchSizeTwo() {
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(2, publisherConfig.getStartTimestamp());

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
    public void test_extractMetaInfo() {
        final Datastore datastore = new Morphia().createDatastore(publisherConfig.getSourceMongoConfig().connectToMongo(),
                                                                  publisherConfig.getSourceMongoConfig().getDbName()
                                                                 );

        final SourceDocumentReferenceMetaInfoDao metaInfoDao = new SourceDocumentReferenceMetaInfoDaoImpl(datastore);

        final List<String> ids = Arrays.asList("902b31943b4e66f5578539bfa60f2b82",
                                               "75a47a2953516381bd9d4d1220bdcfc3",
                                               "46787456ff68f404835064723a4d1cd9",
                                               "cc6103bee8aa27bfa11b851a2377a015",
                                               "764fff8f4654274f9cff28280d7e0008",
                                               "2d74ba5b07344f9587b5c67693fcb3a5"
                                              );

        final List<SourceDocumentReferenceMetaInfo> metaInfos = europeanaDao.retrieveMetaInfo(ids);

        assertEquals(ids.size(), metaInfos.size());

        for (final SourceDocumentReferenceMetaInfo metaInfo: metaInfos) {
            final SourceDocumentReferenceMetaInfo correctMetaInfo = metaInfoDao.read(metaInfo.getId());

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, metaInfo);

        }

    }

    private void checkDocuments (final DBCursor cursor, int batchSize, final DateTime filter) {
        final DBCollection jobStatistics = publisherConfig.getSourceMongoConfig().connectToDB().getCollection("SourceDocumentProcessingStatistics");
        final DBCollection metaInfos = publisherConfig.getSourceMongoConfig().connectToDB().getCollection("SourceDocumentReferenceMetaInfo");
        while (cursor.hasNext()) {
            final List<HarvesterRecord> records = europeanaDao.retrieveRecords(cursor, "");

            assertEquals(batchSize, records.size());

            for (final HarvesterRecord record: records) {
                for (final HarvesterDocument document : record.getAllDocuments()) {
                    if (null != filter) {
                        assertTrue(filter.isBefore(document.getUpdatedAt()));
                    }

                    {
                        final BasicDBObject queryFindDocument = new BasicDBObject();


                        queryFindDocument.put("sourceDocumentReferenceId", document.getSourceDocumentReferenceId());
                        queryFindDocument.put("updatedAt", document.getUpdatedAt().toDate());
                        queryFindDocument.put("referenceOwner.recordId", document.getReferenceOwner().getRecordId());

                        assertEquals(1L, jobStatistics.count(queryFindDocument));
                    }

                    {
                        final BasicDBObject queryFindMetaInfo = new BasicDBObject();

                        queryFindMetaInfo.put("_id", document.getSourceDocumentReferenceId());

                        assertEquals(1L, metaInfos.count(queryFindMetaInfo));
                    }
                }
            }
        }
    }
}
