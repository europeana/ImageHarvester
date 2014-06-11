package eu.europeana.harvester.db.mongo;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import eu.europeana.harvester.db.LinkCheckLimitsDao;
import eu.europeana.harvester.domain.LinkCheckLimits;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

import java.net.UnknownHostException;

public class LinkCheckLimitsDaoImplTest extends TestCase {

    private LinkCheckLimitsDao linkCheckLimitsDao;

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

        linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);
    }

    @Test
    public void testCreate() throws Exception {
        final LinkCheckLimits linkCheckLimits = new LinkCheckLimits(100l, 200l, 300l, 400l);

        assertNotNull(linkCheckLimits.getId());

        linkCheckLimitsDao.create(linkCheckLimits);
        assertEquals(linkCheckLimits.getBandwidthLimitReadInBytesPerSec(),
                linkCheckLimitsDao.read(linkCheckLimits.getId()).getBandwidthLimitReadInBytesPerSec());

        linkCheckLimitsDao.delete(linkCheckLimits);
    }

    @Test
    public void testRead() throws Exception {
        LinkCheckLimits linkCheckLimitsFromRead = linkCheckLimitsDao.read("");
        assertNull(linkCheckLimitsFromRead);

        final LinkCheckLimits linkCheckLimits = new LinkCheckLimits(100l, 200l, 300l, 400l);
        linkCheckLimitsDao.create(linkCheckLimits);
        linkCheckLimitsFromRead = linkCheckLimitsDao.read(linkCheckLimits.getId());

        assertEquals(linkCheckLimits.getBandwidthLimitReadInBytesPerSec(),
                linkCheckLimitsFromRead.getBandwidthLimitReadInBytesPerSec());

        linkCheckLimitsDao.delete(linkCheckLimits);
    }

    @Test
    public void testUpdate() throws Exception {
        final LinkCheckLimits linkCheckLimits = new LinkCheckLimits(100l, 200l, 300l, 400l);
        assertFalse(linkCheckLimitsDao.update(linkCheckLimits));
        linkCheckLimitsDao.create(linkCheckLimits);

        final LinkCheckLimits updatedLinkCheckLimits =
                new LinkCheckLimits(linkCheckLimits.getId(), 400l, 300l, 200l, 100l);
        assertTrue(linkCheckLimitsDao.update(updatedLinkCheckLimits));

        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getBandwidthLimitReadInBytesPerSec(),
                updatedLinkCheckLimits.getBandwidthLimitReadInBytesPerSec());
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getBandwidthLimitWriteInBytesPerSec(),
                updatedLinkCheckLimits.getBandwidthLimitWriteInBytesPerSec());
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getTerminationThresholdSizeLimitInBytes(),
                updatedLinkCheckLimits.getTerminationThresholdSizeLimitInBytes());
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getTerminationThresholdTimeLimit(),
                updatedLinkCheckLimits.getTerminationThresholdTimeLimit());

        linkCheckLimitsDao.delete(updatedLinkCheckLimits);
    }

    @Test
    public void testDelete() throws Exception {
        final LinkCheckLimits linkCheckLimits = new LinkCheckLimits(100l, 200l, 300l, 400l);
        linkCheckLimitsDao.create(linkCheckLimits);

        LinkCheckLimits linkCheckLimitFromRead = linkCheckLimitsDao.read(linkCheckLimits.getId());
        assertNotNull(linkCheckLimitFromRead);

        assertTrue(linkCheckLimitsDao.delete(linkCheckLimits));

        linkCheckLimitFromRead = linkCheckLimitsDao.read(linkCheckLimits.getId());
        assertNull(linkCheckLimitFromRead);

        assertFalse(linkCheckLimitsDao.delete(linkCheckLimits));
    }

    @Test
    public void testCreateOrModify() throws Exception {
        final LinkCheckLimits linkCheckLimits = new LinkCheckLimits(100l, 200l, 300l, 400l);
        assertNull(linkCheckLimitsDao.read(linkCheckLimits.getId()));

        linkCheckLimitsDao.create(linkCheckLimits);
        assertNotNull(linkCheckLimitsDao.read(linkCheckLimits.getId()));
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getBandwidthLimitReadInBytesPerSec(),
                linkCheckLimits.getBandwidthLimitReadInBytesPerSec());

        final LinkCheckLimits updatedLinkCheckLimits =
                new LinkCheckLimits(linkCheckLimits.getId(), 400l, 300l, 200l, 100l);
        linkCheckLimitsDao.createOrModify(updatedLinkCheckLimits);
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getBandwidthLimitReadInBytesPerSec(),
                updatedLinkCheckLimits.getBandwidthLimitReadInBytesPerSec());
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getBandwidthLimitWriteInBytesPerSec(),
                updatedLinkCheckLimits.getBandwidthLimitWriteInBytesPerSec());
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getTerminationThresholdSizeLimitInBytes(),
                updatedLinkCheckLimits.getTerminationThresholdSizeLimitInBytes());
        assertEquals(linkCheckLimitsDao.read(linkCheckLimits.getId()).getTerminationThresholdTimeLimit(),
                updatedLinkCheckLimits.getTerminationThresholdTimeLimit());

        linkCheckLimitsDao.delete(updatedLinkCheckLimits);
    }

}
