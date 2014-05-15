package eu.europeana.harvester.db.mongo;

import com.mongodb.MongoClient;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;
import java.util.Date;

import static org.junit.Assert.*;

public class SourceDocumentProcessingStatisticsDaoImplTest {

    private Datastore datastore;

    @Before
    public void setUp() throws Exception {
        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "europeana";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreate() throws Exception {
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, 1l, 1l, 1l, "", "", 100, "",
                        150*1024l, 50l, 0l, "", null);
        SourceDocumentProcessingStatisticsDaoImpl sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);

        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);
        assertEquals(sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId()).getHttpResponseContentSizeInBytes());

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics);
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentProcessingStatisticsDaoImpl sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);

        SourceDocumentProcessingStatistics sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read("");

        assertNull(sourceDocumentProcessingStatisticsFromRead);

        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, 1l, 1l, 1l, "", "", 100, "",
                        150*1024l, 50l, 0l, "", null);
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);

        sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());

        assertEquals(sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                sourceDocumentProcessingStatisticsFromRead.getHttpResponseContentSizeInBytes());

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics);
    }

    @Test
    public void testUpdate() throws Exception {
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, 1l, 1l, 1l, "", "", 100, "",
                        150*1024l, 50l, 0l, "", null);
        SourceDocumentProcessingStatisticsDaoImpl sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);

        SourceDocumentProcessingStatistics newSourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(sourceDocumentProcessingStatistics.getId(),
                        sourceDocumentProcessingStatistics.getCreatedAt(),
                        sourceDocumentProcessingStatistics.getUpdatedAt(),
                        sourceDocumentProcessingStatistics.getState(),
                        sourceDocumentProcessingStatistics.getProviderId(),
                        sourceDocumentProcessingStatistics.getCollectionId(),
                        sourceDocumentProcessingStatistics.getRecordId(),
                        sourceDocumentProcessingStatistics.getSourceDocumentReferenceId(),
                        sourceDocumentProcessingStatistics.getProcessingJobId(),
                        sourceDocumentProcessingStatistics.getHttpResponseCode(),
                        sourceDocumentProcessingStatistics.getHttpResponseContentType(),
                        sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                        sourceDocumentProcessingStatistics.getRetrievalDurationInSecs(),
                        sourceDocumentProcessingStatistics.getCheckingDurationInSecs(),
                        sourceDocumentProcessingStatistics.getSourceIp(),
                        sourceDocumentProcessingStatistics.getHttpResponseHeaders());

        sourceDocumentProcessingStatisticsDao.update(newSourceDocumentProcessingStatistics);

        assertNotNull(sourceDocumentProcessingStatisticsDao.read(newSourceDocumentProcessingStatistics.getId()));

        sourceDocumentProcessingStatisticsDao.delete(newSourceDocumentProcessingStatistics);
    }

    @Test
    public void testDelete() throws Exception {
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, 1l, 1l, 1l, "", "", 100, "",
                        150*1024l, 50l, 0l, "", null);
        SourceDocumentProcessingStatisticsDaoImpl sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);

        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);

        SourceDocumentProcessingStatistics sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());

        assertNotNull(sourceDocumentProcessingStatisticsFromRead);

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics);

        sourceDocumentProcessingStatisticsFromRead = sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());
        assertNull(sourceDocumentProcessingStatisticsFromRead);
    }
}
