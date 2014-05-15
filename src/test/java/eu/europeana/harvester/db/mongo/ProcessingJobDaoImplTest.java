package eu.europeana.harvester.db.mongo;

import com.mongodb.MongoClient;
import eu.europeana.harvester.domain.JobState;
import eu.europeana.harvester.domain.ProcessingJob;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;
import java.util.Date;

import static org.junit.Assert.*;

public class ProcessingJobDaoImplTest {

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
        ProcessingJob processingJob = new ProcessingJob(new Date(), 1l, 1l, 1l, null, JobState.READY);
        ProcessingJobDaoImpl processingJobDao = new ProcessingJobDaoImpl(datastore);

        processingJobDao.create(processingJob);
        assertEquals(processingJob.getId(), processingJobDao.read(processingJob.getId()).getId());

        processingJobDao.delete(processingJob);
    }

    @Test
    public void testRead() throws Exception {
        ProcessingJobDaoImpl processingJobDao = new ProcessingJobDaoImpl(datastore);
        ProcessingJob processingJob = processingJobDao.read("");

        assertNull(processingJob);
    }

    @Test
    public void testUpdate() throws Exception {
        ProcessingJob processingJob = new ProcessingJob(new Date(), 1l, 1l, 1l, null, JobState.READY);
        ProcessingJobDaoImpl processingJobDao = new ProcessingJobDaoImpl(datastore);

        processingJobDao.create(processingJob);

        assertNotNull(processingJobDao.read(processingJob.getId()));

        ProcessingJob newProcessingJob =
                new ProcessingJob(processingJob.getId(), processingJob.getExpectedStartDate(), processingJob.getProviderId(),
                        processingJob.getCollectionId(), processingJob.getRecordId(),
                        processingJob.getSourceDocumentReferences(), processingJob.getState());

        processingJobDao.update(newProcessingJob);

        assertNotNull(processingJobDao.read(processingJob.getId()));

        processingJobDao.delete(newProcessingJob);
    }

    @Test
    public void testDelete() throws Exception {
        ProcessingJob processingJob = new ProcessingJob(new Date(), 1l, 1l, 1l, null, JobState.READY);
        ProcessingJobDaoImpl processingJobDao = new ProcessingJobDaoImpl(datastore);

        processingJobDao.create(processingJob);

        ProcessingJob processingJobFromRead = processingJobDao.read(processingJob.getId());
        assertNotNull(processingJobFromRead);

        processingJobDao.delete(processingJob);

        processingJobFromRead = processingJobDao.read(processingJob.getId());
        assertNull(processingJobFromRead);
    }
}
