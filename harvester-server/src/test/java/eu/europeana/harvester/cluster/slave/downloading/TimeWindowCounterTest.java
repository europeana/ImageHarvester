package eu.europeana.harvester.cluster.slave.downloading;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import static org.junit.Assert.assertTrue;

public class TimeWindowCounterTest {
    @Rule
    public TestRule watcher = new TestWatcher() {
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }
    };

    @Test
    public void canComputeTheCountsPerSecondsCorrectlyInTheFirstTimeWindow() throws InterruptedException {
        final TimeWindowCounter counter = new TimeWindowCounter();
        counter.start();
        counter.incrementCount(10000);
        Thread.sleep(2000);
        assertTrue(counter.currentTimeWindowRate() >= 4000);
    }
}
