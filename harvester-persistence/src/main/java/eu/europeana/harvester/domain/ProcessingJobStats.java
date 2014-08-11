package eu.europeana.harvester.domain;

import java.util.Map;
import java.util.Set;

/**
 * A convenient collection statistics container.
 */
public class ProcessingJobStats {

    /**
     * Collection stats per record id.
     */
    private final Map<ProcessingState, Set<String>> recordIdsByState;

    /**
     * Collection stats per link id.
     */
    private final Map<ProcessingState, Set<String>> sourceDocumentReferenceIdsByState;

    public ProcessingJobStats(final Map<ProcessingState, Set<String>> recordIdsByState,
                              final Map<ProcessingState, Set<String>> sourceDocumentReferenceIdsByState) {
        this.recordIdsByState = recordIdsByState;
        this.sourceDocumentReferenceIdsByState = sourceDocumentReferenceIdsByState;
    }

    public Map<ProcessingState, Set<String>> getRecordIdsByState() {
        return recordIdsByState;
    }

    public Map<ProcessingState, Set<String>> getSourceDocumentReferenceIdsByState() {
        return sourceDocumentReferenceIdsByState;
    }
}
