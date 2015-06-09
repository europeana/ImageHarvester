package eu.europeana.uimtester.jobcreator.domain;

import java.util.List;

public class UIMTestSample {
    private final String sampleId;
    private final String collectionId;
    private final String providerId;
    private final String recordId;
    private final String executionId;
    private final String edmObjectUrl;
    private final List<String> edmHasViewUrls;
    private final String edmIsShownByUrl;
    private final String edmIsShownAtUrl;

    public UIMTestSample(String sampleId, String collectionId, String providerId, String recordId, String executionId, String edmObjectUrl, List<String> edmHasViewUrls, String edmIsShownByUrl, String edmIsShownAtUrl) {
        this.sampleId = sampleId;
        this.collectionId = collectionId;
        this.providerId = providerId;
        this.recordId = recordId;
        this.executionId = executionId;
        this.edmObjectUrl = edmObjectUrl;
        this.edmHasViewUrls = edmHasViewUrls;
        this.edmIsShownByUrl = edmIsShownByUrl;
        this.edmIsShownAtUrl = edmIsShownAtUrl;
    }

    public String getSampleId() {
        return sampleId;
    }

    public String getCollectionId() {
        return collectionId;
    }

    public String getProviderId() {
        return providerId;
    }

    public String getRecordId() {
        return recordId;
    }

    public String getExecutionId() {
        return executionId;
    }

    public String getEdmObjectUrl() {
        return edmObjectUrl;
    }

    public List<String> getEdmHasViewUrls() {
        return edmHasViewUrls;
    }

    public String getEdmIsShownByUrl() {
        return edmIsShownByUrl;
    }

    public String getEdmIsShownAtUrl() {
        return edmIsShownAtUrl;
    }
}
