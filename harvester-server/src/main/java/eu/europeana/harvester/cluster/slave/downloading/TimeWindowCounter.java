package eu.europeana.harvester.cluster.slave.downloading;

public class TimeWindowCounter {
    private static final int FIVE_MINUTES_IN_SECONDS = 5 * 60;

    private final int timeWindowSizeInSeconds;
    private final int maxLimitCounter;

    private long timestampStartOfCurrentTimeWindow;
    private long countsInCurrentTimeWindow;
    private long countsInPreviousTimeWindow = -1;

    public TimeWindowCounter() {
        this.timeWindowSizeInSeconds = FIVE_MINUTES_IN_SECONDS;
        this.maxLimitCounter = 2000000000;
    }

    public TimeWindowCounter(int timeWindowSizeInSeconds, int maxLimitCounter) {
        this.timeWindowSizeInSeconds = timeWindowSizeInSeconds;
        this.maxLimitCounter = maxLimitCounter;
    }

    public void start() {
        this.timestampStartOfCurrentTimeWindow = System.currentTimeMillis();
        this.countsInCurrentTimeWindow = 0;
    }

    private void reset() {
        this.countsInCurrentTimeWindow = currentTimeWindowRate();
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

    public long previousTimeWindowRate() {
        return countsInPreviousTimeWindow;
    }

    public long currentTimeWindowRate() {
        final long millisSinceStart = System.currentTimeMillis() - timestampStartOfCurrentTimeWindow;
        if (millisSinceStart == 0) return 0;
        return ((int) countsInCurrentTimeWindow / millisSinceStart * 1000);
    }

    public int getTimeWindowSizeInSeconds() {
        return timeWindowSizeInSeconds;
    }
}
