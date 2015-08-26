package eu.europeana.publisher.logic;

import com.codahale.metrics.*;
import eu.europeana.harvester.monitoring.LazyGauge;

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

                public static final Timer mongoGetDocStatisticsDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "retrieveDocStatisticsWithoutMetaInfo", DURATION));
                public static final Timer mongoGetMetaInfoDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "retrieveMetaInfo", DURATION));
                //public static final Timer mongoGetLastDocStatisticsDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "retrieveLastDocStatistics", DURATION));
                public static final Timer mongoGetUrlsDuration = METRIC_REGISTRY.timer(name(Mongo.NAME, "retrieveUrls", DURATION));


                public static final Counter totalNumberOfDocumentsStatistics = METRIC_REGISTRY.counter(name(Mongo.NAME, "numberOfDocumentsStatistics", TOTAL, COUNTER));
                public static final Counter totalNumberOfDocumentsMetaInfo = METRIC_REGISTRY.counter(name(Mongo.NAME, "numberOfDocumentsMetaInfo", TOTAL, COUNTER));

               // public static final Counter totalNumberOfLastDocumentsStatistics = METRIC_REGISTRY.counter(name(Mongo.NAME, "numberOfLastDocumentsStatistics", TOTAL, COUNTER));
               // public static final Counter totalNumberOfLastDocumentsStatisticsWithMetaInfo = METRIC_REGISTRY.counter(name(Mongo.NAME, "numberOfLastDocumentsStatisticsWithMetaInfo", TOTAL, COUNTER));
            }

            public static class Solr {
                public static final String NAME = Read.NAME + ".Solr";

                public static final TimerMap solrCheckIdsDurationDuration = new TimerMap(name(Solr.NAME, "solrCheckIds"));

                public static final Counter totalNumberOfDocumentsThatExistInSolr = METRIC_REGISTRY.counter(name(Solr.NAME, "numberOfDocumentsThatExistInSolr", TOTAL, COUNTER));

                public static final CounterMap totalNumberOfDocumentsThatExistInOneSolr= new CounterMap(name(Solr.NAME, "numberOfDocumentsThatExistInSolr", TOTAL));
            }
        }

        public static class Write {
            public static final String NAME = Publisher.NAME + ".Write";

            public static class Mongo {
                public static final String NAME = Write.NAME + ".Mongo";

                public static final TimerMap mongoWriteDocumentsDuration = new TimerMap(name(Mongo.NAME, "writeDocuments"));
                public static final Counter  totalNumberOfDocumentsWritten = METRIC_REGISTRY.counter(name(Mongo.NAME, "numberOfDocumentsWrittenMongo", TOTAL, COUNTER));

                public static final CounterMap totalNumberOfDocumentsWrittenToOneConnection = new CounterMap(name (Mongo.NAME, "numberOfDocumentsWrittenToMongo", TOTAL));


                public static TimerMap writeEdmObject = new TimerMap(name(Mongo.NAME, "writeEdmObject"));
                public static TimerMap writeEdmPreview = new TimerMap(name(Mongo.NAME, "writeEdmPreview"));
                public static TimerMap mongoWriteMetaInfoDuration = new TimerMap(name(Mongo.NAME, "writeMetaInfo"));
            }

            public static class Solr {
                public static final String NAME = Write.NAME + ".Solr";

                public static final TimerMap solrUpdateDocumentsDuration = new TimerMap(name(Solr.NAME, "updateDocuments"));
                public static final Counter totalNumberOfDocumentsWrittenToSolr = METRIC_REGISTRY.counter(name(Solr.NAME, "numberOfDocumentsWrittenToSolr", TOTAL, COUNTER));

                public static final CounterMap totalNumberOfDocumentsWrittenToOneConnection = new CounterMap(name (Solr.NAME, "numberOfDocumentsWrittenToSolr", TOTAL));
            }
        }

        public static class Batch {
            public static final String NAME = Publisher.NAME + ".Batch";

            public static final TimerMap fakeTagExtraction = new TimerMap(name(Batch.NAME, "fakeTagGeneration", DURATION));
            public static final Timer loopBatchDuration = METRIC_REGISTRY.timer(name(Batch.NAME, "loopBatch", DURATION));
            public static final Counter totalNumberOfInvalidMimeTypes = METRIC_REGISTRY.counter(name(Batch.NAME, "numberOfInvalidMimeTypes", TOTAL, COUNTER));
            public static final Counter totalNumberOfDocumentsWithoutMetaInfo = METRIC_REGISTRY.counter(name(Batch.NAME, "numberOfDocumentsWithoutMetaInfo", TOTAL, COUNTER));
            public static final Counter totalNumberOfDocumentsProcessed = METRIC_REGISTRY.counter(name(Batch.NAME, "numberOfDocumentsProcessed", TOTAL, COUNTER));
            public static final LazyGauge numberOfRemainingDocumentsToProcess = new LazyGauge(METRIC_REGISTRY, name(Batch.NAME, "numberOfRemainingDocumentsToProcess"));
        }
    }
}
