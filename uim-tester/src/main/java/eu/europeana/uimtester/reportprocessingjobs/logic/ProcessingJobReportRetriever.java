package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;
import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessingJobReportRetriever {

    private final HarvesterClient harvesterClient;

    public ProcessingJobReportRetriever(HarvesterClient harvesterClient) {
        this.harvesterClient = harvesterClient;
    }

    public List<ProcessingJobWithStatsAndResults> generateReportOnProcessingJobs(final List<String> jobIds) {
        final List<ProcessingJobWithStatsAndResults> results = new ArrayList<>();

        // Retrieve jobs
        for (final String jobId : jobIds) {
            final ProcessingJob processingJob = harvesterClient.retrieveProcessingJob(jobId);
            final List<SourceDocumentReference> sourceDocumentReferenceList = new ArrayList<>();
            final Map<String, SourceDocumentProcessingStatistics> sourceDocumentReferenceIdToStatsMap = new HashMap();
            final Map<String, SourceDocumentReferenceMetaInfo> sourceDocumentReferenceIdToMetaInfoMap = new HashMap<>();

            for (final String referencedSourceDocumentId : processingJob.getAllReferencedSourceDocumentIds()) {
                final SourceDocumentReference sourceDocumentReference = harvesterClient.retrieveSourceDocumentReferenceById(referencedSourceDocumentId);
                sourceDocumentReferenceList.add(sourceDocumentReference);

                final SourceDocumentProcessingStatistics sourceDocumentProcessingStatistics = harvesterClient.readSourceDocumentProcesssingStatistics(sourceDocumentReference.getId(), processingJob.getId());
                sourceDocumentReferenceIdToStatsMap.put(sourceDocumentReference.getId(), sourceDocumentProcessingStatistics);

                final SourceDocumentReferenceMetaInfo sourceDocumentReferenceMetaInfo = harvesterClient.retrieveMetaInfoByUrl(sourceDocumentReference.getUrl());
                sourceDocumentReferenceIdToMetaInfoMap.put(sourceDocumentReference.getId(), sourceDocumentReferenceMetaInfo);

            }

            results.add(new ProcessingJobWithStatsAndResults(processingJob, sourceDocumentReferenceList, sourceDocumentReferenceIdToStatsMap, sourceDocumentReferenceIdToMetaInfoMap));
        }

        return results;
    }
}
