package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.domain.ReferenceOwner;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Date;

import static org.junit.Assert.*;

public class SourceDocumentProcessingStatisticsDaoImplTest {

    private SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "europeana";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        sourceDocumentProcessingStatisticsDao = new SourceDocumentProcessingStatisticsDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, new ReferenceOwner("1", "1", "1"),
                        "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null);
        assertNotNull(sourceDocumentProcessingStatistics.getId());

        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);
        assertEquals(sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                sourceDocumentProcessingStatisticsDao.read(
                        sourceDocumentProcessingStatistics.getId()).getHttpResponseContentSizeInBytes());

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics);
    }

    @Test
    public void testRead() throws Exception {
        SourceDocumentProcessingStatistics sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read("");
        assertNull(sourceDocumentProcessingStatisticsFromRead);

        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, new ReferenceOwner("1", "1", "1"),
                        "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null);
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);
        sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());

        assertEquals(sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                sourceDocumentProcessingStatisticsFromRead.getHttpResponseContentSizeInBytes());

        sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics);
    }

    @Test
    public void testUpdate() throws Exception {
        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), ProcessingState.DOWNLOADING,
                        new ReferenceOwner("1", "1", "1"), "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null);
        assertFalse(sourceDocumentProcessingStatisticsDao.update(sourceDocumentProcessingStatistics));
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);

        final SourceDocumentProcessingStatistics updatedSourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(sourceDocumentProcessingStatistics.getId(),
                        sourceDocumentProcessingStatistics.getCreatedAt(),
                        sourceDocumentProcessingStatistics.getUpdatedAt(),
                        ProcessingState.SUCCESS,
                        sourceDocumentProcessingStatistics.getReferenceOwner(),
                        sourceDocumentProcessingStatistics.getSourceDocumentReferenceId(),
                        sourceDocumentProcessingStatistics.getProcessingJobId(),
                        sourceDocumentProcessingStatistics.getHttpResponseCode(),
                        sourceDocumentProcessingStatistics.getHttpResponseContentType(),
                        sourceDocumentProcessingStatistics.getHttpResponseContentSizeInBytes(),
                        sourceDocumentProcessingStatistics.getRetrievalDurationInMilliSecs(),
                        sourceDocumentProcessingStatistics.getSocketConnectToDownloadStartDurationInMilliSecs(),
                        sourceDocumentProcessingStatistics.getCheckingDurationInMilliSecs(),
                        sourceDocumentProcessingStatistics.getSourceIp(),
                        sourceDocumentProcessingStatistics.getHttpResponseHeaders());
        assertTrue(sourceDocumentProcessingStatisticsDao.update(updatedSourceDocumentProcessingStatistics));

        assertEquals(sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId()).getState(),
                ProcessingState.SUCCESS);

        sourceDocumentProcessingStatisticsDao.delete(updatedSourceDocumentProcessingStatistics);
    }

    @Test
    public void testDelete() throws Exception {
        final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics =
                new SourceDocumentProcessingStatistics(new Date(), new Date(), null, new ReferenceOwner("1", "1", "1"),
                        "", "", 100, "", 150*1024l, 50l, 0l, 0l, "", null);
        assertFalse(sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics));
        sourceDocumentProcessingStatisticsDao.create(sourceDocumentProcessingStatistics);

        SourceDocumentProcessingStatistics sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());
        assertNotNull(sourceDocumentProcessingStatisticsFromRead);

        assertTrue(sourceDocumentProcessingStatisticsDao.delete(sourceDocumentProcessingStatistics));

        sourceDocumentProcessingStatisticsFromRead =
                sourceDocumentProcessingStatisticsDao.read(sourceDocumentProcessingStatistics.getId());
        assertNull(sourceDocumentProcessingStatisticsFromRead);
    }

}
