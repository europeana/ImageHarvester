package eu.europeana.harvester.cluster.master.metrics;

import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;

import java.util.Map;

/**
 * Created by salexandru on 01.07.2015.
 */
public class ComputeProcessingJobStateStatistics implements Runnable {
    private long numberOfJobsReady;
    private long numberOfJobsSuccessfullyFinished;
    private long numberOfJobsWithError;

    private final SourceDocumentProcessingStatisticsDao processingStatistics;

    public ComputeProcessingJobStateStatistics (final SourceDocumentProcessingStatisticsDao processingStatistics) {
        if (null == processingStatistics)  {
            throw new IllegalArgumentException("SourceDocumentProcessingStatisticsDao cannot be null");
        }

        this.processingStatistics = processingStatistics;
    }

    @Override
    public synchronized void run () {
        final Map<ProcessingState, Long> results = processingStatistics.countNumberOfDocumentsWithState();

        numberOfJobsReady = results.get(ProcessingState.READY);
        numberOfJobsSuccessfullyFinished = results.get(ProcessingState.SUCCESS);
        numberOfJobsWithError = results.get(ProcessingState.ERROR);
    }

    public synchronized long getNumberOfJobsReady () {
        return numberOfJobsReady;
    }

    public synchronized long getNumberOfJobsSuccessfullyFinished () {
        return numberOfJobsSuccessfullyFinished;
    }

    public synchronized long getNumberOfJobsWithError () {
        return numberOfJobsWithError;
    }
}
