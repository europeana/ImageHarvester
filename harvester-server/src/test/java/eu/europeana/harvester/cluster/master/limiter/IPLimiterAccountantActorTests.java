package eu.europeana.harvester.cluster.master.limiter;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import eu.europeana.harvester.cluster.master.limiter.domain.*;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IPLimiterAccountantActorTests {
    private final String ip1 = "192.168.1.1";
    private final String ip2 = "192.168.1.2";
    private final String taskId = "some-task";

    @Test
    public void canReclaimExpiredSlots() throws InterruptedException {
        ActorSystem system = ActorSystem.create();

        new JavaTestKit(system) {{

            final Map<String, Integer> specificLimitsPerIp = new HashMap<>();
            specificLimitsPerIp.put(ip2, 2);

            final ActorRef subject = IPLimiterAccountantActor.createActor(getSystem(), new IPLimiterConfig(1,specificLimitsPerIp,Duration.standardSeconds(2)));

            subject.tell(new ReserveConnectionSlotRequest(ip1,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip1Slot1 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertTrue(ip1Slot1.getGranted());

            subject.tell(new ReserveConnectionSlotRequest(ip1,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip1Slot2 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertFalse(ip1Slot2.getGranted());

            // Here the cleanup will work.
            Thread.sleep(4000);

            subject.tell(new ReserveConnectionSlotRequest(ip1,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip1Slot3 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertTrue(ip1Slot3.getGranted());


        }};
    }


    @Test
    public void canHandleIPLimiterAccountantActorTests() throws InterruptedException {
        ActorSystem system = ActorSystem.create();

        new JavaTestKit(system) {{

            final Map<String, Integer> specificLimitsPerIp = new HashMap<>();
            specificLimitsPerIp.put(ip2, 2);

            final ActorRef subject = IPLimiterAccountantActor.createActor(getSystem(), new IPLimiterConfig(1,specificLimitsPerIp,Duration.standardMinutes(10)));

            // Tests on IP 1
            subject.tell(new ReserveConnectionSlotRequest(ip1,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip1Slot1 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertTrue(ip1Slot1.getGranted());


            subject.tell(new ReserveConnectionSlotRequest(ip1,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip1Slot2 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertFalse(ip1Slot2.getGranted());

            subject.tell(new ReturnConnectionSlotRequest(ip1Slot1.getSlotId(), ip1), getRef());

            subject.tell(new ReserveConnectionSlotRequest(ip1,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip1Slot3 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertTrue(ip1Slot3.getGranted());

            // Tests on IP 2
            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip2Slot1 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertTrue(ip2Slot1.getGranted());

            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip2Slot2 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertTrue(ip2Slot2.getGranted());

            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            final ReserveConnectionSlotResponse ip2Slot3 = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
            assertFalse(ip2Slot3.getGranted());

        }};
    }

    @Test
    public void canHandleIPLimiterAccountantActorWhileLimitsChangeTests() throws InterruptedException {
        ActorSystem system = ActorSystem.create();

        new JavaTestKit(system) {{

            final Map<String, Integer> specificLimitsPerIp = new HashMap<>();
            specificLimitsPerIp.put(ip2, 2);

            final ActorRef subject = IPLimiterAccountantActor.createActor(getSystem(), new IPLimiterConfig(1,specificLimitsPerIp,Duration.standardMinutes(10)));

            // (1) Requests all available slots
            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            assertTrue(expectMsgAnyClassOf(ReserveConnectionSlotResponse.class).getGranted());

            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            assertTrue(expectMsgAnyClassOf(ReserveConnectionSlotResponse.class).getGranted());

            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            assertFalse(expectMsgAnyClassOf(ReserveConnectionSlotResponse.class).getGranted());

            // (2) Increase available slots
            subject.tell(new ChangeMaxAvailableSlotsRequest(ip2,3), getRef());
            Thread.sleep(100);

            // (3) Request more slots
            subject.tell(new ReserveConnectionSlotRequest(ip2,taskId), getRef());
            while (!msgAvailable()) Thread.sleep(100);
            assertTrue(expectMsgAnyClassOf(ReserveConnectionSlotResponse.class).getGranted());

        }};
    }



    @Test(timeout = 10000)
    public void canHandle500kRequestsInUnder10Seconds() throws InterruptedException {

        final DateTime start = DateTime.now();
        final int numberOfIps = 500;
        final int numberOfRequestsPerIp = 1000;
        final List<String> ips = new ArrayList<>();
        for (int i = 0; i < numberOfIps; i++) ips.add(UUID.randomUUID().toString());

        final Map<String, Integer> specificLimitsPerIp = new HashMap<>();
        for (final String ip : ips) specificLimitsPerIp.put(ip, DateTime.now().getMillisOfDay() % 100);

        ActorSystem system = ActorSystem.create();

        new JavaTestKit(system) {
            {

                final ActorRef subject = IPLimiterAccountantActor.createActor(getSystem(), new IPLimiterConfig(1,specificLimitsPerIp,Duration.standardMinutes(10)));
                for (int requests = 0; requests < numberOfIps * numberOfRequestsPerIp; requests++) {
                    final String ip = ips.get(requests % numberOfIps);
                    subject.tell(new ReserveConnectionSlotRequest(ip,taskId), getRef());
                    while (!msgAvailable()) {}
                    final ReserveConnectionSlotResponse response = expectMsgAnyClassOf(ReserveConnectionSlotResponse.class);
                    if (response.getGranted())
                        subject.tell(new ReturnConnectionSlotRequest(response.getSlotId(), ip), getRef());
                }
                System.out.println("Finished in " + new Duration(start, DateTime.now()).getStandardSeconds() + " seconds");

            }
        };
    }
}


