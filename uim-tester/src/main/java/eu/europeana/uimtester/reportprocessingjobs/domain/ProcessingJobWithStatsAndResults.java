package eu.europeana.uimtester.reportprocessingjobs.domain;

import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceMetaInfo;

import java.util.List;
import java.util.Map;

public class ProcessingJobWithStatsAndResults {

    private final ProcessingJob processingJob;

    private final List<SourceDocumentReference> sourceDocumentReferenceList;

    private final Map<String,SourceDocumentProcessingStatistics> sourceDocumentReferenceIdToStatsMap;

    private final Map<String,SourceDocumentReferenceMetaInfo> sourceDocumentReferenceIdTometaInfoMap;

    public ProcessingJobWithStatsAndResults(ProcessingJob processingJob, List<SourceDocumentReference> sourceDocumentReferenceList, Map<String, SourceDocumentProcessingStatistics> sourceDocumentReferenceIdToStatsMap, Map<String, SourceDocumentReferenceMetaInfo> sourceDocumentReferenceIdTometaInfoMap) {
        this.processingJob = processingJob;
        this.sourceDocumentReferenceList = sourceDocumentReferenceList;
        this.sourceDocumentReferenceIdToStatsMap = sourceDocumentReferenceIdToStatsMap;
        this.sourceDocumentReferenceIdTometaInfoMap = sourceDocumentReferenceIdTometaInfoMap;
    }

    public ProcessingJob getProcessingJob() {
        return processingJob;
    }

    public List<SourceDocumentReference> getSourceDocumentReferenceList() {
        return sourceDocumentReferenceList;
    }

    public Map<String, SourceDocumentProcessingStatistics> getSourceDocumentReferenceIdToStatsMap() {
        return sourceDocumentReferenceIdToStatsMap;
    }

    public Map<String, SourceDocumentReferenceMetaInfo> getSourceDocumentReferenceIdTometaInfoMap() {
        return sourceDocumentReferenceIdTometaInfoMap;
    }
}
