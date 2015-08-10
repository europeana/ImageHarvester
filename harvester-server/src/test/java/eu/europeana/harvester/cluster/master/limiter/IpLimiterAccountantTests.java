package eu.europeana.harvester.cluster.master.limiter;

import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotRequest;
import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotResponse;
import eu.europeana.harvester.cluster.master.limiter.domain.ReturnConnectionSlotRequest;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IpLimiterAccountantTests {

    private final String ip1 = "192.168.1.1";
    private final String ip2 = "192.168.1.2";

    @Test
    public void canEnforceConnectionSlotsLimitsOverridenAtIpLevel() {
        final Map<String, Integer> specificLimitsPerIp = new HashMap<>();
        specificLimitsPerIp.put(ip2, 2);

        final IpLimiterAccountant ipLimiterAccountant = new IpLimiterAccountant(1, specificLimitsPerIp);
        final ReserveConnectionSlotResponse ip1Slot1 = ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip1));
        assertTrue(ip1Slot1.getGranted());
        final ReserveConnectionSlotResponse ip1Slot2 = ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip1));
        assertFalse(ip1Slot2.getGranted());

        final ReserveConnectionSlotResponse ip2Slot1 = ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2));
        assertTrue(ip2Slot1.getGranted());
        final ReserveConnectionSlotResponse ip2Slot2 = ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2));
        assertTrue(ip2Slot2.getGranted());
        assertFalse(ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2)).getGranted());
        assertFalse(ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2)).getGranted());
        assertFalse(ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2)).getGranted());
        assertFalse(ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2)).getGranted());
        assertFalse(ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2)).getGranted());
        assertFalse(ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip2)).getGranted());

    }

    @Test(timeout=10000)
    public void canHandle1MillionRequestsInUnder5Seconds() {
        final DateTime start = DateTime.now();
        final int numberOfIps = 1000;
        final int numberOfRequestsPerIp = 1000;
        final List<String> ips = new ArrayList<>();
        for (int i = 0; i< numberOfIps; i++) ips.add(UUID.randomUUID().toString());

        final Map<String, Integer> specificLimitsPerIp = new HashMap<>();
        for (final String ip : ips) specificLimitsPerIp.put(ip, DateTime.now().getMillisOfDay() % 100);
        final IpLimiterAccountant ipLimiterAccountant = new IpLimiterAccountant(1, specificLimitsPerIp);

        for (int requests = 0;requests<numberOfIps*numberOfRequestsPerIp;requests++) {
            final String ip = ips.get(requests % numberOfIps);
            final ReserveConnectionSlotResponse response = ipLimiterAccountant.reserveConnectionSlotRequest(new ReserveConnectionSlotRequest(ip));
            if (response.getGranted()) ipLimiterAccountant.returnConnectionSlotRequest(new ReturnConnectionSlotRequest(response.getSlotId(),ip));
        }
        System.out.println("Finished in " +new Duration(start, DateTime.now()).getStandardSeconds()+" seconds");

    }
}