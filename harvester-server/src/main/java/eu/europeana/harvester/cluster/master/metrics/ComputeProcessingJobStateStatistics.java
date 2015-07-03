package eu.europeana.harvester.cluster.master.metrics;

import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.domain.ProcessingState;

import java.util.Map;

/**
 * Created by salexandru on 01.07.2015.
 */
public class ComputeProcessingJobStateStatistics {
    private long numberOfJobsReady;
    private long numberOfJobsSuccessfullyFinished;
    private long numberOfJobsWithError;

    private final SourceDocumentProcessingStatisticsDao processingStatistics;

    public ComputeProcessingJobStateStatistics (final SourceDocumentProcessingStatisticsDao processingStatistics) {
        if (null == processingStatistics)  {
            throw new IllegalArgumentException("SourceDocumentProcessingStatisticsDao cannot be null");
        }

        this.processingStatistics = processingStatistics;
        this.numberOfJobsReady = 0;
        this.numberOfJobsSuccessfullyFinished = 0;
        this.numberOfJobsWithError = 0;
    }

    public void run () {
        final Map<ProcessingState, Long> results = processingStatistics.countNumberOfDocumentsWithState();
        
        if (null == results || results.isEmpty()) {
            return;
        }

        numberOfJobsReady =  results.containsKey(ProcessingState.READY) ? results.get(ProcessingState.READY) : numberOfJobsReady;
        numberOfJobsSuccessfullyFinished = results.containsKey(ProcessingState.SUCCESS) ? results.get(ProcessingState.SUCCESS) : numberOfJobsSuccessfullyFinished;
        numberOfJobsWithError = results.containsKey(ProcessingState.ERROR) ? results.get(ProcessingState.ERROR) : numberOfJobsWithError;
    }

    public long getNumberOfJobsReady () {
        return numberOfJobsReady;
    }

    public long getNumberOfJobsSuccessfullyFinished () {
        return numberOfJobsSuccessfullyFinished;
    }

    public long getNumberOfJobsWithError () {
        return numberOfJobsWithError;
    }
}
