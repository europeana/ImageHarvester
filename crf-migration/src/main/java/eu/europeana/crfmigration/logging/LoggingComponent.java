package eu.europeana.crfmigration.logging;

import eu.europeana.harvester.domain.LogMarker;
import eu.europeana.harvester.domain.ReferenceOwner;

import java.util.HashMap;
import java.util.Map;

import static net.logstash.logback.marker.Markers.appendEntries;

public class LoggingComponent {

    public static class Migrator {
        public static final String PREFIX = "migrator";
        public static final String PERSISTENCE_EUROPEANA = PREFIX+"."+"persistence"+"."+"europeana";
        public static final String PERSISTENCE_HARVESTER = PREFIX+"."+"persistence"+"."+"harvester";
        public static final String PROCESSING = PREFIX+"."+"processing";
        public static final String PROCESSING_CONVERT_RECORD_TO_JOB = PREFIX+"."+"processing.convertRecordToJob";

    }


    public static final net.logstash.logback.marker.MapEntriesAppendingMarker appendAppFields(final String appComponent) {
        return appendAppFields(appComponent, null, null, null);
    }

    public static final net.logstash.logback.marker.MapEntriesAppendingMarker appendAppFields(final String appComponent, final String migratingBatchId, final String url, final ReferenceOwner referenceOwner) {

        final Map<String, String> map = new HashMap<String, String>();
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_PROVIDER_ID, referenceOwner.getProviderId());
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_COLLECTION_ID, referenceOwner.getCollectionId());
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_RECORD_ID, referenceOwner.getRecordId());
        if (referenceOwner != null)
            map.put(LogMarker.EUROPEANA_PROCESSING_JOB_EXECUTION_ID, referenceOwner.getExecutionId());
        if (url != null) map.put(LogMarker.EUROPEANA_URL, url);
        if (migratingBatchId != null) map.put(LogMarker.EUROPEANA_MIGRATING_BATCH_ID, migratingBatchId);
        if (appComponent != null) map.put(LogMarker.EUROPEANA_COMPONENT, appComponent);
        return appendEntries(map);
    }
}
