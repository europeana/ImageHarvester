package harvester.cluster;

import eu.europeana.harvester.cluster.slave.downloading.TimeWindowCounter;
import org.junit.Test;
import static org.junit.Assert.*;

public class TimeWindowCounterTest {

    @Test
    public void canComputeTheCountsPerSecondsCorrectlyInTheFirstTimeWindow() throws InterruptedException {
        final TimeWindowCounter counter = new TimeWindowCounter();
        counter.start();
        counter.incrementCount(10000);
        Thread.sleep(2000);
        assertEquals(5000,counter.currentTimeWindowRate());
    }


    @Test
    public void canComputeTheCountsPerSecondsCorrectlyInTheSecondTimeWindow() throws InterruptedException {
        final TimeWindowCounter counter = new TimeWindowCounter(2,100000);
        counter.start();
        counter.incrementCount(3000);
        Thread.sleep(2000);
        counter.incrementCount(3000);
        Thread.sleep(1000);
        assertEquals(2000,counter.currentTimeWindowRate());
    }


}
