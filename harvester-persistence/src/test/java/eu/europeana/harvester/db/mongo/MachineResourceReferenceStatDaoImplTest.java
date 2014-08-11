package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.MachineResourceReferenceStatDao;
import eu.europeana.harvester.domain.MachineResourceReferenceStat;
import junit.framework.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import java.net.UnknownHostException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertFalse;

public class MachineResourceReferenceStatDaoImplTest extends TestCase {

    private static final Logger LOG = LogManager.getLogger(MachineResourceReferenceStatDaoImplTest.class.getName());

    private MachineResourceReferenceStatDao machineResourceReferenceStatDao;

    @Before
    public void setUp() throws Exception {
        Datastore datastore = null;

        try {
            MongoClient mongo = new MongoClient("localhost", 27017);
            Morphia morphia = new Morphia();
            String dbName = "europeana";

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.error(e.getMessage());
        }

        machineResourceReferenceStatDao = new MachineResourceReferenceStatDaoImpl(datastore);
    }

    public void testCreate() throws Exception {
        final MachineResourceReferenceStat stats =
                new MachineResourceReferenceStat("192.0.0.1", null, 10.4, null, null, null);
        assertNotNull(stats.getId());

        machineResourceReferenceStatDao.create(stats, WriteConcern.NONE);
        assertEquals(stats.getAvgTime(), machineResourceReferenceStatDao.read(stats.getId()).getAvgTime());

        machineResourceReferenceStatDao.delete(stats.getId());
    }

    public void testRead() throws Exception {
        MachineResourceReferenceStat machineResourceReferenceStatFromRead = machineResourceReferenceStatDao.read("");
        assertNull(machineResourceReferenceStatFromRead);

        final MachineResourceReferenceStat stats =
                new MachineResourceReferenceStat("192.0.0.1", null, 10.4, null, null, null);
        machineResourceReferenceStatDao.create(stats, WriteConcern.NONE);

        machineResourceReferenceStatFromRead = machineResourceReferenceStatDao.read(stats.getId());
        assertNotNull(machineResourceReferenceStatFromRead);

        machineResourceReferenceStatDao.delete(machineResourceReferenceStatFromRead.getId());
    }

    public void testUpdate() throws Exception {
        final MachineResourceReferenceStat stats =
                new MachineResourceReferenceStat("192.0.0.1", null, 10.4, null, null, null);
        assertFalse(machineResourceReferenceStatDao.update(stats, WriteConcern.NONE));
        machineResourceReferenceStatDao.create(stats, WriteConcern.NONE);

        final MachineResourceReferenceStat updatedStats =
                new MachineResourceReferenceStat(stats.getId(), "192.0.0.1", null, 11.1, null, null, null);
        assertTrue(machineResourceReferenceStatDao.update(updatedStats, WriteConcern.NONE));

        assertEquals(machineResourceReferenceStatDao.read(stats.getId()).getAvgTime(), updatedStats.getAvgTime());

        machineResourceReferenceStatDao.delete(updatedStats.getId());
    }

    public void testDelete() throws Exception {
        final MachineResourceReferenceStat stats =
                new MachineResourceReferenceStat("192.0.0.1", null, 10.4, null, null, null);
        machineResourceReferenceStatDao.create(stats, WriteConcern.NONE);

        MachineResourceReferenceStat machineResourceReferenceStatFromRead =
                machineResourceReferenceStatDao.read(stats.getId());
        assertNotNull(machineResourceReferenceStatFromRead);

        assertTrue(machineResourceReferenceStatDao.delete(stats.getId()).getN() == 1);

        machineResourceReferenceStatFromRead = machineResourceReferenceStatDao.read(stats.getId());
        assertNull(machineResourceReferenceStatFromRead);

        assertFalse(machineResourceReferenceStatDao.delete(stats.getId()).getN() == 1);
    }
}
