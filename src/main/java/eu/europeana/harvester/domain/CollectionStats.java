package eu.europeana.harvester.domain;

import java.util.List;
import java.util.Map;

/**
 * A convenient collection statistics container.
 */
public class CollectionStats {

    /**
     * Collection stats per record id.
     */
    private final Map<ProcessingState,List<Long>> recordIdsByState;

    /**
     * Collection stats per link id.
     */
    private final Map<ProcessingState,List<Long>> sourceDocumentReferenceIdsByState;

    public CollectionStats(Map<ProcessingState, List<Long>> recordIdsByState, Map<ProcessingState, List<Long>> sourceDocumentReferenceIdsByState) {
        this.recordIdsByState = recordIdsByState;
        this.sourceDocumentReferenceIdsByState = sourceDocumentReferenceIdsByState;
    }

    public Map<ProcessingState, List<Long>> getRecordIdsByState() {
        return recordIdsByState;
    }

    public Map<ProcessingState, List<Long>> getSourceDocumentReferenceIdsByState() {
        return sourceDocumentReferenceIdsByState;
    }
}
