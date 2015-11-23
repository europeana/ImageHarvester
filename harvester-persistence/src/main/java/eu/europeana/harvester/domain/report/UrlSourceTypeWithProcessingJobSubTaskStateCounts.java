package eu.europeana.harvester.domain.report;

import eu.europeana.harvester.domain.URLSourceType;

import java.util.Map;

public class UrlSourceTypeWithProcessingJobSubTaskStateCounts {
    private final URLSourceType urlSourceType;
    private final Map<SubTaskState,Long> stateCounters;

    public UrlSourceTypeWithProcessingJobSubTaskStateCounts(URLSourceType urlSourceType, Map<SubTaskState, Long> stateCounters) {
        this.urlSourceType = urlSourceType;
        this.stateCounters = stateCounters;
    }

    public URLSourceType getUrlSourceType() {
        return urlSourceType;
    }

    public Map<SubTaskState, Long> getStateCounters() {
        return stateCounters;
    }
}
