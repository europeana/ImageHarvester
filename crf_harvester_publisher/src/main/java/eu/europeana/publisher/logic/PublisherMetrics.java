package eu.europeana.publisher.logic;

import com.codahale.metrics.*;
import scala.io.BytePickle;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by salexandru on 5/6/15.
 */
public class PublisherMetrics  {
    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    public static final String COUNTER = "counter";
    public static final String DURATION = "duration";

    public static final String TOTAL = "total";

    public static class Publisher {
        public static final String NAME = "PublisherMetrics.Publisher";

        public static class Read {
            public static final String NAME = Publisher.NAME + ".Read";

            public static class Mongo {
                public static final String NAME =Read.NAME + ".Mongo";

                public static Timer mongoGetDocStatisticsDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "getDocStatistics", DURATION));
                public static Timer mongoGetMetaInfoDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "getMetaInfo", DURATION));

                public static Counter totalNumberOfDocumentsStatistics = METRIC_REGISTRY.counter(name(Mongo.NAME,
                                                                                                      "numberOfDocumentsStatistics",
                                                                                                      TOTAL, COUNTER));
                public static Counter totalNumberOfDocumentsMetaInfo = METRIC_REGISTRY.counter(name(Mongo.NAME,
                                                                                                      "numberOfDocumentsMetaInfo",
                                                                                                      TOTAL, COUNTER));
            }

            public static class Solr {
                public static final String NAME = Read.NAME + ".Solr";

                public static TimerMap solrCheckIdsDurationDuration = new TimerMap(name(Solr.NAME, "solrCheckIds"));


            }
        }

        public static class Write {
            public static final String NAME = Publisher.NAME + ".Write";

            public static class Mongo {
                public static final String NAME = Write.NAME + ".Mongo";

                public static TimerMap mongoWriteDocumentsDuration = new TimerMap(name(Mongo.NAME, "writeDocuments"));
            }

            public static class Solr {
                public static final String NAME = Write.NAME + ".Solr";

                public static TimerMap solrUpdateDocumentsDuration = new TimerMap(name(Solr.NAME, "updateDocuments"));

            }
        }

        public static class Batch {
            public static final String NAME = Publisher.NAME + ".Batch";

            public static Timer loopBatchDuration = METRIC_REGISTRY.timer(name(Batch.NAME, "loopBatch", DURATION));
        }
    }
}
