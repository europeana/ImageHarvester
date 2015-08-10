package eu.europeana.harvester.cluster.master.limiter;

import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotResponse;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.*;

public class IpConnectionSlotsTests {
    private final String ip1 = "127.0.0.1";

    @Test
    public void canEnforceConnectionSlotsLimits() {
        final IpConnectionSlots ipConnectionSlots = new IpConnectionSlots(2, ip1);
        final ReserveConnectionSlotResponse slot1 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot1.getGranted());
        final ReserveConnectionSlotResponse slot2 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot2.getGranted());
        final ReserveConnectionSlotResponse slot3 = ipConnectionSlots.requestConnectionSlotReservation();
        assertFalse(slot3.getGranted());

    }

    @Test
    public void canReturnConnectionSlots() {
        final IpConnectionSlots ipConnectionSlots = new IpConnectionSlots(2, ip1);
        final ReserveConnectionSlotResponse slot1 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot1.getGranted());
        final ReserveConnectionSlotResponse slot2 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot2.getGranted());
        final ReserveConnectionSlotResponse slot3 = ipConnectionSlots.requestConnectionSlotReservation();
        assertFalse(slot3.getGranted());

        ipConnectionSlots.returnConnectionSlotReservation(slot2.getSlotId());
        ipConnectionSlots.returnConnectionSlotReservation(slot3.getSlotId());

        final ReserveConnectionSlotResponse slot4 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot4.getGranted());
        final ReserveConnectionSlotResponse slot5 = ipConnectionSlots.requestConnectionSlotReservation();
        assertFalse(slot5.getGranted());

    }

    @Test
    public void canCleanExpiredConnectionSlots() throws InterruptedException {
        final IpConnectionSlots ipConnectionSlots = new IpConnectionSlots(2, ip1);
        final ReserveConnectionSlotResponse slot1 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot1.getGranted());
        final ReserveConnectionSlotResponse slot2 = ipConnectionSlots.requestConnectionSlotReservation();
        assertTrue(slot2.getGranted());
        assertEquals(0, ipConnectionSlots.getNumberOfAvailableSlots());
        Thread.sleep(5 * 1000);
        ipConnectionSlots.reclaimOccupiedSlotsOlderThan(DateTime.now().minusSeconds(3));
        assertEquals(2, ipConnectionSlots.getNumberOfAvailableSlots());

    }


}
