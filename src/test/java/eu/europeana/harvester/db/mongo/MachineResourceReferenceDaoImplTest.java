package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.domain.MachineResourceReference;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.*;

public class MachineResourceReferenceDaoImplTest {

    private MachineResourceReferenceDao machineResourceReferenceDao;

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

        machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final MachineResourceReference processingLimits = new MachineResourceReference("1", null, null, 500*1024l, 5l);
        assertNotNull(processingLimits.getId());

        machineResourceReferenceDao.create(processingLimits);
        assertEquals(processingLimits.getBandwidthLimitReadInBytesPerSec(),
                machineResourceReferenceDao.read(processingLimits.getId()).getBandwidthLimitReadInBytesPerSec());

        machineResourceReferenceDao.delete(processingLimits);
    }

    @Test
    public void testRead() throws Exception {
        MachineResourceReference processingLimitsFromRead = machineResourceReferenceDao.read("");
        assertNull(processingLimitsFromRead);

        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1", null, null, 500*1024l, 5l);
        machineResourceReferenceDao.create(machineResourceReference);

        processingLimitsFromRead = machineResourceReferenceDao.read(machineResourceReference.getId());
        assertEquals(machineResourceReference.getBandwidthLimitReadInBytesPerSec(),
                processingLimitsFromRead.getBandwidthLimitReadInBytesPerSec());

        machineResourceReferenceDao.delete(machineResourceReference);
    }

    @Test
    public void testUpdate() throws Exception {
        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1", null, null, 500*1024l, 5l);
        assertFalse(machineResourceReferenceDao.update(machineResourceReference));
        machineResourceReferenceDao.create(machineResourceReference);

        final MachineResourceReference updatedProcessingLimits =
                new MachineResourceReference(machineResourceReference.getId(), null, null, 510*1024l, 5l);
        assertTrue(machineResourceReferenceDao.update(updatedProcessingLimits));

        assertEquals((long) machineResourceReferenceDao.read(
                machineResourceReference.getId()).getBandwidthLimitReadInBytesPerSec(),
                (long)updatedProcessingLimits.getBandwidthLimitReadInBytesPerSec());

        machineResourceReferenceDao.delete(updatedProcessingLimits);
    }

    @Test
    public void testDelete() throws Exception {
        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1", null, null, 500*1024l, 5l);
        machineResourceReferenceDao.create(machineResourceReference);

        MachineResourceReference processingLimitsFromRead =
                machineResourceReferenceDao.read(machineResourceReference.getId());
        assertNotNull(processingLimitsFromRead);

        assertTrue(machineResourceReferenceDao.delete(machineResourceReference));

        processingLimitsFromRead = machineResourceReferenceDao.read(machineResourceReference.getId());
        assertNull(processingLimitsFromRead);

        assertFalse(machineResourceReferenceDao.delete(machineResourceReference));
    }

    @Test
    public void testCreateOrModify() throws Exception {
        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1", null, null, 500*1024l, 5l);
        assertNull(machineResourceReferenceDao.read(machineResourceReference.getId()));

        machineResourceReferenceDao.createOrModify(machineResourceReference);
        assertNotNull(machineResourceReferenceDao.read(machineResourceReference.getId()));
        assertEquals(machineResourceReferenceDao.read(
                machineResourceReference.getId()).getBandwidthLimitReadInBytesPerSec(),
                machineResourceReference.getBandwidthLimitReadInBytesPerSec());

        final MachineResourceReference updatedProcessingLimits =
                new MachineResourceReference(machineResourceReference.getId(), null, null, 10240*1024l, 5l);
        machineResourceReferenceDao.createOrModify(updatedProcessingLimits);
        assertEquals(machineResourceReferenceDao.read(
                machineResourceReference.getId()).getBandwidthLimitReadInBytesPerSec(),
                updatedProcessingLimits.getBandwidthLimitReadInBytesPerSec());
        assertNotEquals(machineResourceReferenceDao.read(
                machineResourceReference.getId()).getBandwidthLimitReadInBytesPerSec(),
                machineResourceReference.getBandwidthLimitReadInBytesPerSec());

        machineResourceReferenceDao.delete(updatedProcessingLimits);
    }

}
