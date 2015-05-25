package eu.europeana.JobCreator.domain;

import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;

public class ProcessingJobTuple {

    private final ProcessingJob processingJob;
    private final SourceDocumentReference sourceDocumentReference;

    public ProcessingJobTuple(ProcessingJob processingJob, SourceDocumentReference sourceDocumentReference) {
        this.processingJob = processingJob;
        this.sourceDocumentReference = sourceDocumentReference;
    }

    public ProcessingJob getProcessingJob() {
        return processingJob;
    }

    public SourceDocumentReference getSourceDocumentReference() {
        return sourceDocumentReference;
    }
}
