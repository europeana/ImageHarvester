package eu.europeana.harvester.client.report;

import eu.europeana.harvester.domain.URLSourceType;

import java.util.Map;

public class UrlSourceTypeWithProcessingJobSubTaskStateCounts {
    private final URLSourceType urlSourceType;
    private final Map<SubTaskState,Integer> stateCounters;

    public UrlSourceTypeWithProcessingJobSubTaskStateCounts(URLSourceType urlSourceType, Map<SubTaskState, Integer> stateCounters) {
        this.urlSourceType = urlSourceType;
        this.stateCounters = stateCounters;
    }

    public URLSourceType getUrlSourceType() {
        return urlSourceType;
    }

    public Map<SubTaskState, Integer> getStateCounters() {
        return stateCounters;
    }
}
