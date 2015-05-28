package harvester.cluster;

import eu.europeana.harvester.cluster.slave.downloading.TimeWindowCounter;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by paul on 28/05/15.
 */
public class TimeWindowCounterTest {

    @Test
    public void canRun() throws InterruptedException {
        final TimeWindowCounter counter = new TimeWindowCounter();
        counter.start();
        counter.incrementCount(1000);
        Thread.sleep(1000);
        assertEquals(counter.countsPerSecond(),1000);
    }
}
