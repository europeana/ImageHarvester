package eu.europeana.harvester.cluster.master;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.domain.ProcessingState;
import eu.europeana.harvester.httpclient.response.RetrievingState;

import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by paul on 04/06/15.
 */
public class MasterMetrics {
    public static final String COUNTER = "counter";
    public static final String DURATION = "duration";

    public static final String TOTAL = "total";

    public static final String SEND_JOBS_SET_TO_SLAVE = "sendJobsSetToSlave";

    public static final String DONE_DOWNLOAD = "doneDownload";
    public static final String DONE_PROCESSING = "doneProcessing";

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();


    public static class Master {
        public static final String NAME = "MasterMetrics" + "." + "Master";
        public static final Timer sendJobSetToSlaveDuration = METRIC_REGISTRY.timer(name(Master.NAME, SEND_JOBS_SET_TO_SLAVE, DURATION));
        public static final Counter sendJobSetToSlaveCounter = METRIC_REGISTRY.counter(name(Master.NAME, SEND_JOBS_SET_TO_SLAVE, COUNTER));


        public static final Map<RetrievingState, Meter> doneDownloadStateCounters = new HashMap();

        static {
            for (final RetrievingState state : RetrievingState.values()) {
                doneDownloadStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_DOWNLOAD, state.name(), COUNTER)));
            }
        }

        public static final Meter doneDownloadTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_DOWNLOAD, TOTAL, COUNTER));

        public static final Map<ProcessingState, Meter> doneProcessingStateCounters = new HashMap();

        static {
            for (final ProcessingState state : ProcessingState.values()) {
                doneProcessingStateCounters.put(state, METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING, state.name(), COUNTER)));
            }
        }

        public static final Meter doneProcessingTotalCounter = METRIC_REGISTRY.meter(name(Master.NAME, DONE_PROCESSING, TOTAL, COUNTER));

    }
}
