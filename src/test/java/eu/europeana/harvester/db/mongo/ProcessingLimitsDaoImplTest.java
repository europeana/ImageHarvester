package eu.europeana.harvester.db.mongo;

import com.mongodb.MongoClient;
import eu.europeana.harvester.domain.ProcessingLimits;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class ProcessingLimitsDaoImplTest {

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
        ProcessingLimits processingLimits = new ProcessingLimits(1l, 500*1024l, 5l);
        ProcessingLimitsDaoImpl processingLimitsDao = new ProcessingLimitsDaoImpl(datastore);

        processingLimitsDao.create(processingLimits);
        assertEquals(processingLimits.getBandwidthLimitReadInBytesPerSec(),
                processingLimitsDao.read(processingLimits.getId()).getBandwidthLimitReadInBytesPerSec());

        processingLimitsDao.delete(processingLimits);
    }

    @Test
    public void testRead() throws Exception {
        ProcessingLimitsDaoImpl processingLimitsDao = new ProcessingLimitsDaoImpl(datastore);
        ProcessingLimits processingLimitsFromRead = processingLimitsDao.read("");

        assertNull(processingLimitsFromRead);

        ProcessingLimits processingLimits = new ProcessingLimits(1l, 500*1024l, 5l);
        processingLimitsDao.create(processingLimits);

        processingLimitsFromRead = processingLimitsDao.read(processingLimits.getId());

        assertEquals(processingLimits.getBandwidthLimitReadInBytesPerSec(),
                processingLimitsFromRead.getBandwidthLimitReadInBytesPerSec());

        processingLimitsDao.delete(processingLimits);
    }

    @Test
    public void testUpdate() throws Exception {
        ProcessingLimits processingLimits = new ProcessingLimits(1l, 500*1024l, 5l);
        ProcessingLimitsDaoImpl processingLimitsDao = new ProcessingLimitsDaoImpl(datastore);

        processingLimitsDao.create(processingLimits);

        ProcessingLimits newProcessingLimits = new ProcessingLimits(processingLimits.getId(), 1l, 510*1024l, 5l);
        processingLimitsDao.update(newProcessingLimits);

        assertEquals((long)processingLimitsDao.read(processingLimits.getId()).getBandwidthLimitReadInBytesPerSec(), 510 * 1024l);

        processingLimitsDao.delete(newProcessingLimits);
    }

    @Test
    public void testDelete() throws Exception {
        ProcessingLimits processingLimits = new ProcessingLimits(1l, 500*1024l, 5l);
        ProcessingLimitsDaoImpl processingLimitsDao = new ProcessingLimitsDaoImpl(datastore);

        processingLimitsDao.create(processingLimits);

        ProcessingLimits processingLimitsFromRead = processingLimitsDao.read(processingLimits.getId());
        assertNotNull(processingLimitsFromRead);

        processingLimitsDao.delete(processingLimits);

        processingLimitsFromRead = processingLimitsDao.read(processingLimits.getId());
        assertNull(processingLimitsFromRead);
    }
}
