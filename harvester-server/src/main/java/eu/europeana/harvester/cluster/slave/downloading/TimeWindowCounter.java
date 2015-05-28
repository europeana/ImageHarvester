package eu.europeana.harvester.cluster.slave.downloading;

public class TimeWindowCounter {
    private static final int FIVE_MINUTES_IN_SECONDS = 5*60;

    private final int timeWindowSizeInSeconds;
    private final int maxLimitCounter;

    private long timestampStartOfCurrentTimeWindow;
    private long countsInCurrentTimeWindow;

    public TimeWindowCounter() {
        this.timeWindowSizeInSeconds = FIVE_MINUTES_IN_SECONDS;
        this.maxLimitCounter = 2000000000;
    }

    public TimeWindowCounter(int timeWindowSizeInSeconds, int maxLimitCounter) {
        this.timeWindowSizeInSeconds = timeWindowSizeInSeconds;
        this.maxLimitCounter = maxLimitCounter;
    }

    public void start() {
        reset();
    }

        private void reset() {
        this.timestampStartOfCurrentTimeWindow = System.currentTimeMillis();
        this.countsInCurrentTimeWindow = 0;
    }

    public void incrementCount(final int incrementCount) {
        this.countsInCurrentTimeWindow += incrementCount;
        if ((System.currentTimeMillis() - timestampStartOfCurrentTimeWindow * 1000 >= timeWindowSizeInSeconds * 1000) ||
                (countsInCurrentTimeWindow >= maxLimitCounter)) {
            reset();
        }
    }

    public long countsPerSecond() {
        return ((int) countsInCurrentTimeWindow / ((System.currentTimeMillis() - timestampStartOfCurrentTimeWindow) / 1000)) ;
    }

    public int getTimeWindowSizeInSeconds() {
        return timeWindowSizeInSeconds;
    }
}
