package eu.europeana.harvester.logging;

import eu.europeana.harvester.domain.LogMarker;
import eu.europeana.harvester.domain.ReferenceOwner;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

import static net.logstash.logback.marker.Markers.appendEntries;

public class LoggingComponent {

    public static class Master {
        public static final String PREFIX_MASTER = "harvester.master";

        public static final String TASKS_ACCOUNTANT = PREFIX_MASTER+"."+"accountant";
        public static final String TASKS_LOADER = PREFIX_MASTER+"."+"loader";
        public static final String TASKS_RECEIVER = PREFIX_MASTER+"."+"receiver";
        public static final String TASKS_SENDER = PREFIX_MASTER+"."+"senderr";

    }

    public static class Slave {
        public static final String PREFIX = "harvester.slave";
        public static final String SUPERVISOR = PREFIX + "." + "nodeSupervisor";
        public static final String MASTER = PREFIX + "." + "nodeMaster";
        public static final String SLAVE_RETRIEVAL = PREFIX + "." + "slave.retrieval";
        public static final String SLAVE_PROCESSING = PREFIX + "." + "slave.processing";
    }


    public static final net.logstash.logback.marker.MapEntriesAppendingMarker appendAppFields(final Logger LOG, final String appComponent) {
        return appendAppFields(LOG, appComponent, null, null, null);
    }

    public static final net.logstash.logback.marker.MapEntriesAppendingMarker appendAppFields(final Logger LOG, final String appComponent, final String jobId, final String url, final ReferenceOwner referenceOwner) {

        final Map<String, String> map = new HashMap<String, String>();
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_PROVIDER_ID, referenceOwner.getProviderId());
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_COLLECTION_ID, referenceOwner.getCollectionId());
        if (referenceOwner != null) map.put(LogMarker.EUROPEANA_RECORD_ID, referenceOwner.getRecordId());
        if (referenceOwner != null)
            map.put(LogMarker.EUROPEANA_PROCESSING_JOB_EXECUTION_ID, referenceOwner.getExecutionId());
        if (url != null) map.put(LogMarker.EUROPEANA_URL, url);
        if (jobId != null) map.put(LogMarker.EUROPEANA_PROCESSING_JOB_ID, jobId);
        if (appComponent != null) map.put(LogMarker.EUROPEANA_COMPONENT, appComponent);
        return appendEntries(map);
    }
}
