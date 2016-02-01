package eu.europeana.publisher.dao;

import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.ProcessingJobSubTaskState;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.HarvesterRecord;
import eu.europeana.publisher.domain.PublisherConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;
import utilities.ConfigUtils;
import utilities.MongoDatabase;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by salexandru on 09.06.2015.
 */
public class PublisherHarvesterDaoTest {

//    private static final String DATA_PATH_PREFIX = "/Users/paul/Documents/workspace/ImageHarvester/crf_harvester_publisher/src/test/resources/data-files/";
//    private static final String CONFIG_PATH_PREFIX = "/Users/paul/Documents/workspace/ImageHarvester/crf_harvester_publisher/src/test/resources/config-files/";

    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private PublisherConfig publisherConfig;

    private PublisherHarvesterDao harvesterDao;
    private List<WebResourceMetaInfo> correctMetaInfos;
    private List<HarvesterRecord> harvesterDocuments;

    private WebResourceMetaInfoDao webResourceMetaInfoDao;

    private MongoDatabase mongoDatabase = null;

    @Before
    public void setUp() throws IOException {
        publisherConfig = ConfigUtils.createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");
        mongoDatabase = new MongoDatabase(publisherConfig);
        harvesterDao = new PublisherHarvesterDao(publisherConfig.getTargetDBConfig().get(0));

        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        mongoDatabase.loadMongoData(publisherConfig.getTargetDBConfig().get(0).getMongoConfig(), DATA_PATH_PREFIX + "aggregation.json", "Aggregation");
        mongoDatabase.loadMongoData(publisherConfig.getTargetDBConfig().get(0).getMongoConfig(), DATA_PATH_PREFIX + "europeanaAggregation.json", "EuropeanaAggregation");
        mongoDatabase.loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "sourceDocumentReference.json", "SourceDocumentReference");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(100, null);

        correctMetaInfos = new ArrayList<>();
        harvesterDocuments = new ArrayList<>();
        harvesterDocuments = europeanaDao.retrieveRecords(cursor, "");

        for (final HarvesterRecord record: harvesterDocuments) {
            for (final SourceDocumentReferenceMetaInfo metaInfo: record.getUniqueMetainfos()) {
                final WebResourceMetaInfo webResourceMetaInfo = new WebResourceMetaInfo(
                    metaInfo.getId(),
                    metaInfo.getImageMetaInfo(),
                    metaInfo.getAudioMetaInfo(),
                    metaInfo.getVideoMetaInfo(),
                    metaInfo.getTextMetaInfo()
                );

                correctMetaInfos.add (webResourceMetaInfo);
            }
        }

        final Morphia morphia = new Morphia();
        webResourceMetaInfoDao = new WebResourceMetaInfoDaoImpl(europeanaDao.getMongoDB(),morphia,
            morphia.createDatastore(publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToMongo(),
                    publisherConfig.getTargetDBConfig().get(0).getMongoConfig().getDbName()
            )
        );
    }

    @After
    public void tearDown() {
        mongoDatabase.cleanMongoDatabase();
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_NullConfig() throws UnknownHostException {
        new PublisherHarvesterDao(null);
    }

    @Test
    public void test_Save_NullElements() {
        harvesterDao.writeMetaInfos(null);
        assertEquals(0, publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB().getCollection("WebResourceMetaInfo").count());
    }

    @Test
    public void test_Save_EmptyElements() {
        harvesterDao.writeMetaInfos(Collections.EMPTY_LIST);
        assertEquals(0, publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB().getCollection("WebResourceMetaInfo").count());
    }

    @Test
    @Ignore
    public void test_Write_OneElement () {
        harvesterDao.writeMetaInfos(harvesterDocuments.subList(0, 1));
        final DB db = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB();

        int idx = 0;
        for (final HarvesterRecord record: harvesterDocuments.subList(0, 1)) {
            for (final SourceDocumentReferenceMetaInfo metaInfo : record.getUniqueMetainfos()) {
                final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(metaInfo.getId());

                final WebResourceMetaInfo correctMetaInfo = correctMetaInfos.get(idx++);

                ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);
            }

            final HarvesterDocument document = record.getEdmIsShownByDocument();

            if (null != document) {
                final String edmObject = (String) db.getCollection("Aggregation").findOne(
                      new BasicDBObject("about", "/aggregation/provider" + document.getReferenceOwner().getRecordId()))
                                       .get("edmObject");
                assertEquals(document.getUrl(), edmObject);
            }
        }
    }

    @Test
    @Ignore
    public void test_Write_TwoElements () {
        harvesterDao.writeMetaInfos(harvesterDocuments.subList(0, 2));
        final DB db = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB();

        int idx = 0;
        for (final HarvesterRecord record: harvesterDocuments.subList(0, 1)) {
            for (final SourceDocumentReferenceMetaInfo metaInfo : record.getUniqueMetainfos()) {
                final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(metaInfo.getId());

                final WebResourceMetaInfo correctMetaInfo = correctMetaInfos.get(idx++);

                ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);
            }

            final HarvesterDocument document = record.getEdmIsShownByDocument();

            if (null != document) {
                final String edmObject = (String) db.getCollection("Aggregation").findOne(
                                                                                                 new BasicDBObject("about", "/aggregation/provider" + document.getReferenceOwner().getRecordId()))
                                                    .get("edmObject");
                assertEquals(document.getUrl(), edmObject);
            }
        }
    }

    @Test
    public void test_Write_AllElements () {
        harvesterDao.writeMetaInfos(harvesterDocuments);
        final DB db = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB();

        int idx = 0;
        for (final HarvesterRecord record: harvesterDocuments.subList(0, 1)) {
            for (final SourceDocumentReferenceMetaInfo metaInfo : record.getUniqueMetainfos()) {
                final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(metaInfo.getId());

                final WebResourceMetaInfo correctMetaInfo = correctMetaInfos.get(idx++);

                ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);
            }

            final HarvesterDocument document = record.getEdmIsShownByDocument();

            if (null == document) {
                continue;
            }

            final String edmObject = (String)db.getCollection("Aggregation").findOne(new BasicDBObject("about", "/aggregation/provider" + document.getReferenceOwner().getRecordId())).get("edmObject");
            final String edmPreview = (String)db.getCollection("EuropeanaAggregation").findOne(new BasicDBObject("about", "/aggregation/europeana" + document.getReferenceOwner().getRecordId())).get("edmPreview");


            if (null != document &&
                 ProcessingJobSubTaskState.SUCCESS == document.getSubTaskStats().getThumbnailGenerationState() &&
                 ProcessingJobSubTaskState.SUCCESS == document.getSubTaskStats().getThumbnailStorageState()) {

                assertEquals(document.getUrl(), edmObject);
                assertEquals(document.getUrl(), edmPreview);
            }
            else {
                assertEquals("ana are mere", edmObject);
                assertEquals("ana are mere", edmPreview);
            }
        }
    }
}
