package eu.europeana.publisher.dao;

import com.google.code.morphia.Morphia;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.*;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;
import utilities.ConfigUtils;
import utilities.DButils;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import static org.junit.Assert.assertEquals;
import static utilities.DButils.loadMongoData;

/**
 * Created by salexandru on 09.06.2015.
 */
public class PublisherHarvesterDaoTest {

    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private PublisherConfig publisherConfig;

    private PublisherHarvesterDao harvesterDao;
    private List<WebResourceMetaInfo> correctMetaInfos;
    private List<HarvesterDocument> harvesterDocuments;

    private WebResourceMetaInfoDao webResourceMetaInfoDao;

    @Before
    public void setUp() throws IOException {
        publisherConfig = ConfigUtils.createPublisherConfig(CONFIG_PATH_PREFIX + "publisher.conf");
        harvesterDao = new PublisherHarvesterDao(publisherConfig.getTargetDBConfig().get(0));

        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json", "SourceDocumentReferenceMetaInfo");
        loadMongoData(publisherConfig.getTargetDBConfig().get(0).getMongoConfig(), DATA_PATH_PREFIX + "aggregation.json", "Aggregation");
        loadMongoData(publisherConfig.getTargetDBConfig().get(0).getMongoConfig(), DATA_PATH_PREFIX + "europeanaAggregation.json", "EuropeanaAggregation");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "sourceDocumentReference.json", "SourceDocumentReference");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        correctMetaInfos = new ArrayList<>();
        harvesterDocuments = new ArrayList<>();
        harvesterDocuments = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, cursor.count());

        final ListIterator<HarvesterDocument> documentIterator = harvesterDocuments.listIterator();

        while (documentIterator.hasNext()) {
            final HarvesterDocument document = documentIterator.next();

            if (document.getTaskType() == DocumentReferenceTaskType.CHECK_LINK ||
                ProcessingJobSubTaskState.SUCCESS != document.getSubTaskStats().getMetaExtractionState()
               ) {
               documentIterator.remove();
            }
        }

        for (final HarvesterDocument document: harvesterDocuments) {
            if (null == document.getSourceDocumentReferenceMetaInfo()) continue;
            final WebResourceMetaInfo webResourceMetaInfo = new WebResourceMetaInfo(
              document.getSourceDocumentReferenceMetaInfo().getId(),
              document.getSourceDocumentReferenceMetaInfo().getImageMetaInfo(),
              document.getSourceDocumentReferenceMetaInfo().getAudioMetaInfo(),
              document.getSourceDocumentReferenceMetaInfo().getVideoMetaInfo(),
              document.getSourceDocumentReferenceMetaInfo().getTextMetaInfo()
            );

            correctMetaInfos.add (webResourceMetaInfo);
        }

        webResourceMetaInfoDao = new WebResourceMetaInfoDaoImpl(
            new Morphia().createDatastore(publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToMongo(),
                                          publisherConfig.getTargetDBConfig().get(0).getMongoConfig().getDbName()
                                         )
        );
    }

    @After
    public void tearDown() {
       DButils.cleanMongoDatabase(publisherConfig);
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
    public void test_Write_OneElement () {
        harvesterDao.writeMetaInfos(harvesterDocuments.subList(0, 1));
        final DB db = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB();

        int idx = 0;
        for (final HarvesterDocument document: harvesterDocuments.subList(0, 1)) {
            final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(document.getSourceDocumentReferenceMetaInfo().getId());

            final WebResourceMetaInfo correctMetaInfo = correctMetaInfos.get(idx++);

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);

            if (document.getUrlSourceType() == URLSourceType.ISSHOWNBY) {
               final String edmObject = (String)db.getCollection("Aggregation").findOne(new BasicDBObject("about", "/aggregation/provider" + document.getReferenceOwner().getRecordId())).get("edmObject");
               assertEquals(document.getUrl(), edmObject);
            }
        }
    }

    @Test
    public void test_Write_TwoElements () {
        harvesterDao.writeMetaInfos(harvesterDocuments.subList(0, 2));
        final DB db = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB();

        int idx = 0;
        for (final HarvesterDocument document: harvesterDocuments.subList(0, 2)) {
            final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(document.getSourceDocumentReferenceMetaInfo().getId());

            final WebResourceMetaInfo correctMetaInfo =correctMetaInfos.get(idx++);

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);

            if (document.getUrlSourceType() == URLSourceType.ISSHOWNBY) {
                final String edmObject = (String)db.getCollection("Aggregation").findOne(new BasicDBObject("about", "/aggregation/provider" + document.getReferenceOwner().getRecordId())).get("edmObject");
                assertEquals(document.getUrl(), edmObject);
            }
        }
    }

    @Test
    public void test_Write_AllElements () {
        harvesterDao.writeMetaInfos(harvesterDocuments);
        final DB db = publisherConfig.getTargetDBConfig().get(0).getMongoConfig().connectToDB();

        int idx = 0;
        for (final HarvesterDocument document: harvesterDocuments) {
            final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(document.getSourceDocumentReferenceMetaInfo()
                                                                                            .getId());

            final WebResourceMetaInfo correctMetaInfo = correctMetaInfos.get(idx++);

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);

            final String edmObject = (String)db.getCollection("Aggregation").findOne(new BasicDBObject("about", "/aggregation/provider" + document.getReferenceOwner().getRecordId())).get("edmObject");


            final String edmPreview = (String)db.getCollection("EuropeanaAggregation").findOne(new BasicDBObject("about", "/aggregation/europeana" + document.getReferenceOwner().getRecordId())).get("edmPreview");

            if (URLSourceType.ISSHOWNBY == document.getUrlSourceType() &&
                ProcessingJobSubTaskState.SUCCESS == document.getSubTaskStats().getThumbnailGenerationState() &&
                ProcessingJobSubTaskState.SUCCESS == document.getSubTaskStats().getThumbnailStorageState()) {
                assertEquals(document.getUrl(), edmObject);
                assertEquals(document.getUrl(), edmPreview);
            }
            else {
                assertEquals ("ana are mere", edmObject);
                assertEquals ("ana are mere", edmPreview);
            }
        }
    }


}
