package eu.europeana.harvester.cluster.slave.processing.exceptiions;

import eu.europeana.harvester.domain.ProcessingJobSubTaskStats;

/**
 * Created by salexandru on 29.07.2015.
 */
public class LocaleException extends RuntimeException {
    private final ProcessingJobSubTaskStats subTaskStats;

    public LocaleException (ProcessingJobSubTaskStats subTaskStats, Throwable throwable) {
        super (throwable);
        this.subTaskStats = subTaskStats;
    }

    public ProcessingJobSubTaskStats getSubTaskStats () {
        return subTaskStats;
    }
}
