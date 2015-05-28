package eu.europeana.harvester.cluster.slave.downloading;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TimeWindowCounterTest {

    @Test
    public void canComputeTheCountsPerSecondsCorrectlyInTheFirstTimeWindow() throws InterruptedException {
        final TimeWindowCounter counter = new TimeWindowCounter();
        counter.start();
        counter.incrementCount(10000);
        Thread.sleep(2000);
        assertTrue(counter.currentTimeWindowRate() >= 4000);
    }
}
