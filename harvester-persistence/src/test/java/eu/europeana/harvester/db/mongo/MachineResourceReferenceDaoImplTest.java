package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.domain.MachineResourceReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.*;

import static org.junit.Assert.*;

public class MachineResourceReferenceDaoImplTest {

    private static final Logger LOG = LogManager.getLogger(MachineResourceReferenceDaoImplTest.class.getName());

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
            LOG.error(e.getMessage());
        }

        machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final MachineResourceReference processingLimits = new MachineResourceReference("1");
        assertNotNull(processingLimits.getId());

        machineResourceReferenceDao.create(processingLimits, WriteConcern.NONE);
        machineResourceReferenceDao.delete(processingLimits.getId());
    }

    @Test
    public void testRead() throws Exception {
        MachineResourceReference processingLimitsFromRead = machineResourceReferenceDao.read("");
        assertNull(processingLimitsFromRead);

        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1");
        machineResourceReferenceDao.create(machineResourceReference, WriteConcern.NONE);

        processingLimitsFromRead = machineResourceReferenceDao.read(machineResourceReference.getId());
        machineResourceReferenceDao.delete(machineResourceReference.getId());
    }

    @Test
    public void testUpdate() throws Exception {
        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1");
        assertFalse(machineResourceReferenceDao.update(machineResourceReference, WriteConcern.NONE));
        machineResourceReferenceDao.create(machineResourceReference, WriteConcern.NONE);

        final MachineResourceReference updatedProcessingLimits =
                new MachineResourceReference(machineResourceReference.getId());
        assertTrue(machineResourceReferenceDao.update(updatedProcessingLimits, WriteConcern.NONE));

        machineResourceReferenceDao.delete(updatedProcessingLimits.getId());
    }

    @Test
    public void testDelete() throws Exception {
        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1");
        machineResourceReferenceDao.create(machineResourceReference, WriteConcern.NONE);

        MachineResourceReference processingLimitsFromRead =
                machineResourceReferenceDao.read(machineResourceReference.getId());
        assertNotNull(processingLimitsFromRead);

        assertTrue(machineResourceReferenceDao.delete(machineResourceReference.getId()).getN() == 1);

        processingLimitsFromRead = machineResourceReferenceDao.read(machineResourceReference.getId());
        assertNull(processingLimitsFromRead);

        assertFalse(machineResourceReferenceDao.delete(machineResourceReference.getId()).getN() == 1);
    }

    @Test
    public void testCreatOrModify_NullCollection() {
        assertFalse(machineResourceReferenceDao.createOrModify((Collection) null, WriteConcern.NONE).iterator()
                                               .hasNext());
    }

    @Test
    public void testCreatOrModify_EmptyCollection() {
        assertFalse(machineResourceReferenceDao.createOrModify((Collection) null, WriteConcern.NONE).iterator().hasNext());
    }



    @Test
    public void testCreateOrModify_OneElement() throws Exception {
        final MachineResourceReference machineResourceReference =
                new MachineResourceReference("1");
        assertNull(machineResourceReferenceDao.read(machineResourceReference.getId()));

        machineResourceReferenceDao.createOrModify(machineResourceReference, WriteConcern.NONE);
        assertNotNull(machineResourceReferenceDao.read(machineResourceReference.getId()));

        machineResourceReferenceDao.delete(machineResourceReference.getId());
    }

    @Test
    public void testCreateOrModify_TwoElements() throws Exception {
        final List<MachineResourceReference> machineResourceReference =
                Arrays.asList(
                    new MachineResourceReference("1"),
                    new MachineResourceReference("2")
                );

        for (int i = 1; i < 3; ++i) {
            assertNull(machineResourceReferenceDao.read(Integer.toString(i)));
        }

        machineResourceReferenceDao.createOrModify(machineResourceReference, WriteConcern.NONE);

        for (int i = 1; i < 3; ++i) {
            assertNotNull(machineResourceReferenceDao.read(Integer.toString(i)));
            machineResourceReferenceDao.delete(Integer.toString(i));
        }
    }

    @Test
    public void testCreateOrModify_ManyElements() throws Exception {
        final List<MachineResourceReference> machineResourceReference = new ArrayList<>();
        final int size = new Random().nextInt(1000) + 50;

        for (int i = 0; i < size; ++i) {
            assertNull(machineResourceReferenceDao.read(Integer.toString(i)));
            machineResourceReference.add(new MachineResourceReference(Integer.toString(i)));
        }

        machineResourceReferenceDao.createOrModify(machineResourceReference, WriteConcern.NONE);

        for (int i = 0; i < size; ++i) {
            assertNotNull(machineResourceReferenceDao.read(Integer.toString(i)));
            machineResourceReferenceDao.delete(Integer.toString(i));
        }
    }



}
