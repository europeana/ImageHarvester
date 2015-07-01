package eu.europeana.harvester.cluster.master.metrics;

import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by salexandru on 01.07.2015.
 */
public class ComputeProcessingJobStateStatistics implements Runnable {
    private AtomicLong numberOfJobsReady = new AtomicLong();
    private AtomicLong numberOfJobsSuccessfulyFinished = new AtomicLong();
    private AtomicLong numberOfJobsWithError = new AtomicLong();

    private final SourceDocumentProcessingStatisticsDao processingStatistics;

    public ComputeProcessingJobStateStatistics (SourceDocumentProcessingStatisticsDao processingStatistics) {this.processingStatistics = processingStatistics;}

    @Override
    public void run () {
        final Map<ProcessingState, Integer> results = processingStatistics.countNumberOfDocumentsWithState();

        numberOfJobsReady.set(results.get(ProcessingState.READY));
        numberOfJobsSuccessfulyFinished.set(results.get(ProcessingState.SUCCESS));
        numberOfJobsWithError.set(results.get(ProcessingState.ERROR));
    }

    public Long getNumberOfJobsReady () {
        return numberOfJobsReady.longValue();
    }

    public Long getNumberOfJobsSuccessfulyFinished () {
        return numberOfJobsSuccessfulyFinished.longValue();
    }

    public Long getNumberOfJobsWithError () {
        return numberOfJobsWithError.longValue();
    }
}
