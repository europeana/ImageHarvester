package eu.europeana.publisher.dao;

import com.google.code.morphia.Morphia;
import com.mongodb.DBCursor;
import eu.europeana.harvester.db.interfaces.WebResourceMetaInfoDao;
import eu.europeana.harvester.db.mongo.WebResourceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.WebResourceMetaInfo;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.domain.HarvesterDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.unitils.reflectionassert.ReflectionAssert;
import utilities.ConfigUtils;
import utilities.DButils;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static utilities.DButils.connectToDB;
import static utilities.DButils.loadMongoData;

/**
 * Created by salexandru on 09.06.2015.
 */
public class PublisherHarvesterDaoTest {

    private static final String DATA_PATH_PREFIX = "./src/test/resources/data-files/";
    private static final String CONFIG_PATH_PREFIX = "./src/test/resources/config-files/";

    private static final PublisherConfig publisherConfig = ConfigUtils.createPublisherConfig(CONFIG_PATH_PREFIX +
                                                                                                     "publisher.conf");

    private PublisherHarvesterDao harvesterDao;
    private List<WebResourceMetaInfo> correctMetaInfos;
    private List<HarvesterDocument> harvesterDocuments;

    private WebResourceMetaInfoDao webResourceMetaInfoDao;

    @Before
    public void setUp() throws UnknownHostException {
        harvesterDao = new PublisherHarvesterDao(publisherConfig.getTargetMongoConfig());

        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "jobStatistics.json", "SourceDocumentProcessingStatistics");
        loadMongoData(publisherConfig.getSourceMongoConfig(), DATA_PATH_PREFIX + "metaInfo.json", "SourceDocumentReferenceMetaInfo");

        final PublisherEuropeanaDao europeanaDao = new PublisherEuropeanaDao(publisherConfig.getSourceMongoConfig());
        final DBCursor cursor = europeanaDao.buildCursorForDocumentStatistics(null);

        correctMetaInfos = new ArrayList<>();
        harvesterDocuments = new ArrayList<>();
        harvesterDocuments = europeanaDao.retrieveDocumentsWithMetaInfo(cursor, cursor.count());

        for (final HarvesterDocument document: harvesterDocuments) {
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
            new Morphia().createDatastore(connectToDB(publisherConfig.getTargetMongoConfig()).getMongo(),
                                          publisherConfig.getTargetMongoConfig().getdBName()
                                         )
        );
    }

    @After
    public void tearDown() {
        DButils.cleanMongoDatabase(publisherConfig.getSourceMongoConfig(), publisherConfig.getTargetMongoConfig());
    }

    @Test (expected = IllegalArgumentException.class)
    public void test_NullConfig() throws UnknownHostException {
        new PublisherHarvesterDao(null);
    }

    @Test
    public void test_Save_NullElements() {
        harvesterDao.writeMetaInfos(null);
        assertEquals(0, DButils.connectToDB(publisherConfig.getTargetMongoConfig()).getCollection("WebResourceMetaInfo")
                               .count());
    }

    @Test
    public void test_Save_EmptyElements() {
        harvesterDao.writeMetaInfos(Collections.EMPTY_LIST);
        assertEquals(0, DButils.connectToDB(publisherConfig.getTargetMongoConfig())
                               .getCollection("WebResourceMetaInfo").count());
    }

    @Test
    public void test_Write_OneElement () {
        harvesterDao.writeMetaInfos(harvesterDocuments.subList(0, 1));

        int idx = 0;
        for (final HarvesterDocument document: harvesterDocuments.subList(0, 1)) {
            final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(document.getSourceDocumentReferenceMetaInfo().getId());

            final WebResourceMetaInfo correctMetaInfo = correctMetaInfos.get(idx++);

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);
        }
    }

    @Test
    public void test_Write_TwoElements () {
        harvesterDao.writeMetaInfos(harvesterDocuments.subList(0, 2));

        int idx = 0;
        for (final HarvesterDocument document: harvesterDocuments.subList(0, 2)) {
            final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(document.getSourceDocumentReferenceMetaInfo().getId());

            final WebResourceMetaInfo correctMetaInfo =correctMetaInfos.get(idx++);

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);
        }
    }

    @Test
    public void test_Write_AllElements () {
        harvesterDao.writeMetaInfos(harvesterDocuments);

        int idx = 0;
        for (final HarvesterDocument document: harvesterDocuments) {
            final WebResourceMetaInfo writtenMetaInfo = webResourceMetaInfoDao.read(document.getSourceDocumentReferenceMetaInfo().getId());

            final WebResourceMetaInfo correctMetaInfo =correctMetaInfos.get(idx++);

            ReflectionAssert.assertReflectionEquals(correctMetaInfo, writtenMetaInfo);

        }
    }
}
