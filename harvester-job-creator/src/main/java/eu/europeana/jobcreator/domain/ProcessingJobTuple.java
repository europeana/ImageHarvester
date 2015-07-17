package eu.europeana.jobcreator.domain;

import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.harvester.domain.SourceDocumentReferenceProcessingProfile;

import java.util.ArrayList;
import java.util.List;

public class ProcessingJobTuple {

    public static final List<ProcessingJob> processingJobsFromList(final List<ProcessingJobTuple> tuples) {
        final List<ProcessingJob> result = new ArrayList();
        for (final ProcessingJobTuple tuple : tuples) result.add(tuple.getProcessingJob());
        return result;
    }

    public static final List<SourceDocumentReference> sourceDocumentReferencesFromList(final List<ProcessingJobTuple> tuples) {
        final List<SourceDocumentReference> result = new ArrayList();
        for (final ProcessingJobTuple tuple : tuples) result.add(tuple.getSourceDocumentReference());
        return result;
    }

    private final ProcessingJob processingJob;
    private final SourceDocumentReference sourceDocumentReference;
    private final List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles;

    public ProcessingJobTuple(ProcessingJob processingJob, SourceDocumentReference sourceDocumentReference, List<SourceDocumentReferenceProcessingProfile> sourceDocumentReferenceProcessingProfiles) {
        this.processingJob = processingJob;
        this.sourceDocumentReference = sourceDocumentReference;
        this.sourceDocumentReferenceProcessingProfiles = sourceDocumentReferenceProcessingProfiles;
    }

    public ProcessingJob getProcessingJob() {
        return processingJob;
    }

    public SourceDocumentReference getSourceDocumentReference() {
        return sourceDocumentReference;
    }


    public List<SourceDocumentReferenceProcessingProfile> getSourceDocumentReferenceProcessingProfiles() {
        return sourceDocumentReferenceProcessingProfiles;
    }
}
