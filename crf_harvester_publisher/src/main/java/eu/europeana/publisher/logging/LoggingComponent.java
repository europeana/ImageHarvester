package eu.europeana.publisher.logging;

import eu.europeana.harvester.domain.LogMarker;
import eu.europeana.harvester.domain.ReferenceOwner;

import java.util.HashMap;
import java.util.Map;

import static net.logstash.logback.marker.Markers.appendEntries;

public class LoggingComponent {

    public static class Migrator {
        public static final String PREFIX = "migrator";
        public static final String PERSISTENCE_SOLR = PREFIX+"."+"persistence"+"."+"solr";
        public static final String PERSISTENCE_HARVESTER = PREFIX+"."+"persistence"+"."+"harvester";
        public static final String PERSISTENCE_EUROPEANA = PREFIX+"."+"persistence"+"."+"europeana";

        public static final String PROCESSING = PREFIX+"."+"processing";
        public static final String PROCESSING_TAG_EXTRACTOR = PREFIX+"."+"processing"+"."+"tagextraction";
    }


    public static final net.logstash.logback.marker.MapEntriesAppendingMarker appendAppFields(final String appComponent) {
        return appendAppFields(appComponent, null, null, null);
    }

    public static final net.logstash.logback.marker.MapEntriesAppendingMarker appendAppFields(final String appComponent, final String publishingBatchId, final String url, final ReferenceOwner referenceOwner) {

        final Map<String, String> map = new HashMap<String, String>();
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_PROVIDER_ID, referenceOwner.getProviderId());
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_COLLECTION_ID, referenceOwner.getCollectionId());
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_RECORD_ID, referenceOwner.getRecordId());
        if (referenceOwner != null)
            map.put(LogMarker.EUROPEANA_PROCESSING_JOB_EXECUTION_ID, referenceOwner.getExecutionId());
        if (url != null) map.put(LogMarker.EUROPEANA_URL, url);
        if (publishingBatchId != null) map.put(LogMarker.EUROPEANA_PUBLISHING_BATCH_ID, publishingBatchId);
        if (appComponent != null) map.put(LogMarker.EUROPEANA_COMPONENT, appComponent);
        return appendEntries(map);
    }
}
