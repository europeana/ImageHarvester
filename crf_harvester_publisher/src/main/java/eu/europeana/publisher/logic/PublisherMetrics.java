package eu.europeana.publisher.logic;

import com.codahale.metrics.*;
import eu.europeana.harvester.monitoring.LazyGauge;
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

                public static final Timer mongoGetDocStatisticsDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "getDocStatistics", DURATION));
                public static final Timer mongoGetMetaInfoDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "getMetaInfo", DURATION));

                public static final Counter totalNumberOfDocumentsStatistics = METRIC_REGISTRY.counter(name(Mongo.NAME,
                                                                                                      "numberOfDocumentsStatistics",
                                                                                                      TOTAL, COUNTER));
                public static final Counter totalNumberOfDocumentsMetaInfo = METRIC_REGISTRY.counter(name(Mongo.NAME,
                                                                                                      "numberOfDocumentsMetaInfo",
                                                                                                      TOTAL, COUNTER));
            }

            public static class Solr {
                public static final String NAME = Read.NAME + ".Solr";

                public static final TimerMap solrCheckIdsDurationDuration = new TimerMap(name(Solr.NAME, "solrCheckIds"));

                public static final Counter totalNumberOfDocumentsThatExistInSolr = METRIC_REGISTRY.counter(name(Solr.NAME, TOTAL, "numberOfDocumentsThatExistInSolr", COUNTER));

            }
        }

        public static class Write {
            public static final String NAME = Publisher.NAME + ".Write";

            public static class Mongo {
                public static final String NAME = Write.NAME + ".Mongo";

                public static final TimerMap mongoWriteDocumentsDuration = new TimerMap(name(Mongo.NAME, "writeDocuments"));
                public static final Counter  totalNumberOfDocumentsWritten = METRIC_REGISTRY.counter(name(Mongo.NAME, TOTAL, "numberOfDocumentsWrittenMongo", COUNTER));
            }

            public static class Solr {
                public static final String NAME = Write.NAME + ".Solr";

                public static final TimerMap solrUpdateDocumentsDuration = new TimerMap(name(Solr.NAME, "updateDocuments"));
                public static final Counter totalNumberOfDocumentsWrittenToSolr = METRIC_REGISTRY.counter(name(Solr.NAME, TOTAL, "numberOfDocumentsWrittenToSolr", COUNTER));


            }
        }

        public static class Batch {
            public static final String NAME = Publisher.NAME + ".Batch";

            public static final Timer loopBatchDuration = METRIC_REGISTRY.timer(name(Batch.NAME, "loopBatch", DURATION));
            public static final Counter totalNumberOfInvalidMimetypes = METRIC_REGISTRY.counter(name(Batch.NAME, TOTAL,
                                                                                                     "numberOfInvalidMimetypes",
                                                                                                     COUNTER));
            public static final Counter totalNumberOfDocumentsWithoutMetaInfo = METRIC_REGISTRY.counter(name(Batch.NAME, TOTAL, "numberOfDocumentsWithoutMetaInfo", COUNTER));


            public static final Counter totalNumberOfDocumentsProcessed = METRIC_REGISTRY.counter(name(Batch.NAME, TOTAL, "numberOfDocumentsProcessed", COUNTER));
            public static final LazyGauge numberOfRemaningDocumentsToProcess = new LazyGauge(METRIC_REGISTRY, name(Batch.NAME, "numberOfRemaningDocumentsToProcess"));
        }
    }
}
