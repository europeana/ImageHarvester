package eu.europeana.publisher.logic;

import com.codahale.metrics.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Created by salexandru on 5/6/15.
 */
public class PublisherMetrics  {
    public static final MetricRegistry metricRegistry = new MetricRegistry();

    private final Map<String, Metric> metrics;

    /**
     *  Timers
     */
    private final Timer  mongoGetDocStatTimer;
    private final Timer  mongoGetMetaInfoTimer;
    private final Timer  mongoWriteTimer;
    private final Timer  solrCheckIdsTimer;
    private final Timer  solrUpdateTimer;
    private final Timer  loopBatchTimer;
    private final Timer  totalTimer;
    private final Map<Timer, Timer.Context> timerContexts;


    /**
     *  Counters
     */
    private final Counter totalNumberOfDocumentsPublished;
    private final Counter totalNumberOfInvalidMimetypes;
    private final Counter totalNumberOfDocumentsWithoutMetaInfo;
    private final Counter totalNumberOfDocumentsProcessed;
    private final Counter totalNumberOfMetaInfoDocumentsRetrived;
    private final Counter totalNumberOfDocumentsThatExistInSolr;
    /**
     * Meters
     */
    private final Meter numberOfDocumentsPublished;
    private final Meter numberOfDocumentsProcessed;
    private final Meter numberOfMetaInfoDocumentsRetrived;
    private final Meter numberOfDocumentsThatExistInSolr;



    public PublisherMetrics() {
        metrics = new HashMap<>();
        timerContexts = new HashMap<>();


        totalTimer              = metricRegistry.timer(name(PublisherMetrics.class, "Total Execution Time"));
        mongoGetDocStatTimer    = metricRegistry.timer(name(PublisherMetrics.class, "Mongo get SourceDocumentProcessingStatistics"));
        mongoGetMetaInfoTimer   = metricRegistry.timer(name(PublisherMetrics.class, "Mongo get SourceDocumentMetaInfo"));
        mongoWriteTimer         = metricRegistry.timer(name(PublisherMetrics.class, "Mongo write WebResourceMetaInfo"));
        solrCheckIdsTimer       = metricRegistry.timer(name(PublisherMetrics.class, "Solr get ids for documents"));
        solrUpdateTimer         = metricRegistry.timer(name(PublisherMetrics.class, "Solr write metadata"));
        loopBatchTimer          = metricRegistry.timer(name(PublisherMetrics.class, "Loop Batch Processing"));

        totalNumberOfDocumentsPublished = metricRegistry.counter(name(PublisherMetrics.class, "Total Number of Documents Published"));
        totalNumberOfDocumentsProcessed = metricRegistry.counter(name(PublisherMetrics.class, "Total Number of Documents Processed"));
        totalNumberOfInvalidMimetypes   = metricRegistry.counter(name(PublisherMetrics.class, "Total Number of Invalid Mimetypes"));
        totalNumberOfDocumentsWithoutMetaInfo = metricRegistry.counter(name(PublisherMetrics.class, "Total Number Of Documents without MetaInfo"));
        totalNumberOfMetaInfoDocumentsRetrived = metricRegistry.counter(name(PublisherMetrics.class, "Total Number of MetaInfo Documents retrived"));
        totalNumberOfDocumentsThatExistInSolr  = metricRegistry.counter(name(PublisherMetrics.class, "Total Number of Documents That Exist in Solr"));

        numberOfDocumentsProcessed = metricRegistry.meter(name(PublisherMetrics.class, "Number of Documents Processed"));
        numberOfDocumentsPublished = metricRegistry.meter(name(PublisherMetrics.class, "Number of Documents Published"));
        numberOfMetaInfoDocumentsRetrived = metricRegistry.meter(name(PublisherMetrics.class, "Number of MetaInfo Documents retrieved"));
        numberOfDocumentsThatExistInSolr = metricRegistry.meter(name(PublisherMetrics.class, "Number Of Documents That Exist in Solr"));
    }

    public void logNumberOfMetaInfoDocumentsRetrived(long inc) {
        totalNumberOfMetaInfoDocumentsRetrived.inc(inc);
        numberOfMetaInfoDocumentsRetrived.mark(inc);
    }

    public void logNumberOfDocumentsProcessed(long inc) {
        totalNumberOfDocumentsProcessed.inc(inc);
        numberOfDocumentsProcessed.mark(inc);
    }

    public void logNumberOfDocumentsPublished(long inc) {
        totalNumberOfDocumentsPublished.inc(inc);
        numberOfDocumentsPublished.mark(inc);
    }

    public void incTotalNumberOfInvalidMimetypes() {
        totalNumberOfInvalidMimetypes.inc();
    }

    public void incTotalNumberOfDocumentsWithoutMetaInfo() {
        totalNumberOfDocumentsWithoutMetaInfo.inc();
    }

    public Collection<Timer.Context> getTimerContexts() {
        return timerContexts.values();
    }

    public void startTotalTimer() {
        startTimer(totalTimer);
    }

    public void stopTotalTimer() {
        stopTimer(totalTimer);
    }

    public void startMongoGetDocStatTimer() {
        startTimer(mongoGetDocStatTimer);
    }

    public void stopMongoGetDocStatTimer() {
        stopTimer(mongoGetDocStatTimer);
    }

    public void startMongoGetMetaInfoTimer() {
        startTimer(mongoGetMetaInfoTimer);
    }

    public void stopMongoGetMetaInfoTimer() {
        stopTimer(mongoGetMetaInfoTimer);
    }

    public void startMongoWriteTimer() {
        startTimer(mongoWriteTimer);
    }

    public void stopMongoWriteTimer() {
        stopTimer(mongoWriteTimer);
    }

    public void startSolrReadIdsTimer() {
        startTimer(solrCheckIdsTimer);
    }

    public void stopSolrReadIdsTimer() {
        stopTimer(solrCheckIdsTimer);
    }

    public void startSolrUpdateTimer() {
        startTimer(solrUpdateTimer);
    }

    public void stopSolrUpdateTimer() {
        stopTimer(solrUpdateTimer);
    }

    public void startLoopBatchTimer() {
        startTimer(loopBatchTimer);
    }

    public void stopLoopBatchTimer() {
        stopLoopBatchTimer();
    }

    public <T> void registerGauge(final String gaugeName, final Gauge<T> gauge) {
        metricRegistry.register(name(PublisherMetrics.class, gaugeName), gauge);
    }

    private void startTimer(Timer timer) {
        stopTimer(timer);
        timerContexts.put(timer, timer.time());
    }

    private void stopTimer(Timer timer) {
        if (timerContexts.containsKey(timer)) {
            timerContexts.remove(timer).stop();
        }
    }


    public void logNumberOfDocumentsThatExistInSolr(int inc) {
        totalNumberOfDocumentsThatExistInSolr.inc(inc);
        numberOfDocumentsThatExistInSolr.mark(inc);
    }
}
