package eu.europeana.publisher.logic;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mongodb.DBCursor;
import eu.europeana.publisher.dao.PublisherEuropeanaDao;
import eu.europeana.publisher.dao.PublisherWriter;
import eu.europeana.publisher.domain.*;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.extract.FakeTagExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * It's responsible for the whole publishing process. It's the engine of the
 * publisher module.
 */
public class PublisherManager {
    private static final int MAX_RETRIES_CURSOR_REBUILD = 5;

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final PublisherConfig config;

    private final PublisherWriter[] writers;

    private PublisherEuropeanaDao publisherEuropeanaDao;

    private DateTime currentTimestamp;

    private Boolean shouldStopGracefully = false;

    public PublisherManager(PublisherConfig config) throws UnknownHostException {
        this.config = config;

        writers = new PublisherWriter[config.getTargetDBConfig().size()];

        int count = 0;
        for (final DBTargetConfig targetConfig : config.getTargetDBConfig()) {
            writers[count++] = new PublisherWriter(targetConfig);
        }

        publisherEuropeanaDao = new PublisherEuropeanaDao(config.getSourceMongoConfig());

        currentTimestamp = config.getStartTimestamp();

//  Enable if you want statistics in logs
        Slf4jReporter reporter = Slf4jReporter.forRegistry(PublisherMetrics.METRIC_REGISTRY)
                                              .outputTo(LOG)
                                              .convertRatesTo(TimeUnit.SECONDS)
                                              .convertDurationsTo(TimeUnit.MILLISECONDS).build();

        reporter.start(20, TimeUnit.SECONDS);

        if (StringUtils.isEmpty(config.getGraphiteConfig().getServer())) {
            return;
        }

        final InetSocketAddress addr = new InetSocketAddress(config.getGraphiteConfig().getServer(),
                config.getGraphiteConfig().getPort());
        Graphite graphite = new Graphite(addr);
        GraphiteReporter reporter2 = GraphiteReporter.forRegistry(PublisherMetrics.METRIC_REGISTRY)
                .prefixedWith(config.getGraphiteConfig().getMasterId())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .filter(MetricFilter.ALL)
                .build(graphite);
        reporter2.start(20, TimeUnit.SECONDS);
    }


    public void start() throws IOException, SolrServerException, InterruptedException {
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                "Starting publishing process. The minimal timestamp is {}", config.getStartTimestamp());

        final AtomicLong numberOfDocumentToProcess = new AtomicLong();
        final String publishingBatchId = "publishing-batch-" + DateTime.now().getMillis() + "-" + Math.random();

        final Runnable runGauge = new Runnable() {
            @Override
            public void run() {
                numberOfDocumentToProcess.set(publisherEuropeanaDao.countNumberOfDocumentUpdatedBefore(currentTimestamp));
                LOG.debug(LoggingComponent
                                .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                        "Number of Remaining documents to process is: " + numberOfDocumentToProcess.toString());
            }
        };


        runGauge.run();

        PublisherMetrics.Publisher.Batch.numberOfRemainingDocumentsToProcess.registerHandler(new Gauge<Long>() {
            @Override
            public Long getValue() {
                return numberOfDocumentToProcess.longValue();
            }
        });

        PublisherMetrics.Publisher.Batch.timestamp.registerHandler(new Gauge<Date>() {
            @Override
            public Date getValue() {
                return null == currentTimestamp ? new Date(0) : currentTimestamp.toDate();
            }
        });

        while (true) {

            final Timer.Context context = PublisherMetrics.Publisher.Batch.loopBatchDuration.time();
            try {
                batchProcessing();

                shouldStopGracefully = shouldPublisherStopGraceFully();

                LOG.debug(LoggingComponent
                                .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                        "ShouldStopGracefully is " + shouldStopGracefully);


                if (shouldStopGracefully == true) {
                    LOG.debug(LoggingComponent
                                    .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                            "Gracefully stopping publisher at end of batch as stop request received during batch processing. Last OK processed timestamp {}", currentTimestamp);
                    return;
                } else {
                    LOG.debug(LoggingComponent
                                    .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                            "Continuing with next batch as shouldStopGracefully is " + shouldStopGracefully);
                }

            } catch (Exception e) {
                LOG.debug(LoggingComponent
                                .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                        "Stopping publisher as current batch ended prematurely with error. Last OK processed timestamp {}", currentTimestamp);

                throw e;
            } finally {
                context.close();
            }
        }
    }

    private void batchProcessing() throws InterruptedException, IOException, SolrServerException {
        final String publishingBatchId = "publishing-batch-" + DateTime.now().getMillis() + "-" + Math.random();

        DBCursor cursor = publisherEuropeanaDao.buildCursorForDocumentStatistics(config.getBatch(), currentTimestamp);

        List<HarvesterRecord> retrievedDocs = null;

        do {
            retrievedDocs = publisherEuropeanaDao.retrieveRecords(cursor, publishingBatchId);

            LOG.debug(LoggingComponent
                            .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                    "Retrieved CRF records {}", (null == retrievedDocs ? 0 : retrievedDocs.size()));

            if (null == retrievedDocs || retrievedDocs.isEmpty()) {
                LOG.error("received null or empty records from source mongo. Sleeping " + config.getSleepSecondsAfterEmptyBatch());
                TimeUnit.SECONDS.sleep(config.getSleepSecondsAfterEmptyBatch());
                cursor.close();
                cursor = publisherEuropeanaDao.buildCursorForDocumentStatistics(config.getBatch(), currentTimestamp);
            }
        } while (null == retrievedDocs || retrievedDocs.isEmpty());

        cursor.close();


        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                "Starting filtering and retrieved records and saving them to solr and mongo");

        PublisherMetrics.Publisher.Batch.totalNumberOfDocumentsProcessed.inc(retrievedDocs.size());
        for (final PublisherWriter writer : writers) {
            final String newPublishingBatchId = "publishing-batch-" + writer.getConnectionId() + "-" + DateTime.now().getMillis() + "-" + Math.random();
            LOG.debug(LoggingComponent
                            .appendAppFields(LoggingComponent.Migrator.PROCESSING, newPublishingBatchId, null, null),
                    "Starting filtering current pair of solr/mongo write config id {}", writer.getConnectionId());

            final List<HarvesterRecord> document = writer.getSolrWriter().filterDocumentIds(retrievedDocs, publishingBatchId);
            LOG.debug(LoggingComponent
                            .appendAppFields(LoggingComponent.Migrator.PROCESSING, newPublishingBatchId, null, null),
                    "Retrieved CRF records after SOLR filtering {} for config id {}", document.size(),
                    writer.getConnectionId());

            LOG.debug(LoggingComponent
                            .appendAppFields(LoggingComponent.Migrator.PROCESSING, newPublishingBatchId, null, null),
                    "Starting extracting tags for current pair of solr/mongo write config id {}",
                    writer.getConnectionId());

            List<CRFSolrDocument> crfSolrDocument = new ArrayList<>();

            final Timer.Context context = PublisherMetrics.Publisher.Batch.fakeTagExtraction.time(writer.getConnectionId());
            try {
                crfSolrDocument = FakeTagExtractor.extractTags(document, newPublishingBatchId);
            } finally {
                context.stop();
            }

            if (null != crfSolrDocument && !crfSolrDocument.isEmpty()) {
                LOG.debug(LoggingComponent
                                .appendAppFields(LoggingComponent.Migrator.PROCESSING, newPublishingBatchId, null,
                                        null), "Started updating metainfos for config id {}",
                        writer.getConnectionId());

                writer.getHarvesterDao().writeMetaInfos(document, publishingBatchId);

                LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING, newPublishingBatchId, null, null),
                        "Updating solr documents for config id {}",
                        writer.getConnectionId());

                writer.getSolrWriter().updateDocuments(crfSolrDocument, publishingBatchId);
            } else {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING, newPublishingBatchId, null, null),
                        "There was a problem with writing this batch to solr for connection id {}. " +
                                "No meta info was written to mongo. Maybe documents where empty ?",
                        writer.getConnectionId()
                );
            }
        }

        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                "Done with a batch of writing and filtering for all solr/mongo pairs");

        currentTimestamp = updateTimestamp(currentTimestamp, retrievedDocs);
        LOG.debug(LoggingComponent
                        .appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                "Updating currentTime to: " + currentTimestamp);

        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING, publishingBatchId, null, null),
                "Updating timestamp after batch finished to " + currentTimestamp);

    }

    private boolean shouldPublisherStopGraceFully() throws IOException {
        if (!Files.exists(Paths.get(config.getStopGracefullyFile()))) {
            LOG.debug(LoggingComponent
                            .appendAppFields(LoggingComponent.Migrator.PROCESSING, "", null, null),
                    "No stop graceful file found at  " + config.getStopGracefullyFile() + ".");

            return false;
        }
        final String fileContent = new String(Files.readAllBytes(Paths.get(config.getStopGracefullyFile())));
        LOG.debug(LoggingComponent
                        .appendAppFields(LoggingComponent.Migrator.PROCESSING, "", null, null),
                "Stop graceful file found at  " + config.getStopGracefullyFile() + " with content " + fileContent);

        return ("true".equalsIgnoreCase(fileContent.trim()));
    }


    private DateTime updateTimestamp(final DateTime currentTime, final Collection<HarvesterRecord> records) throws
            IOException {
        DateTime time = currentTime;

        for (final HarvesterRecord record : records) {
            for (final HarvesterDocument document : record.getAllDocuments()) {
                if (null == document.getUpdatedAt()) continue;
                if (null == time) {
                    time = document.getUpdatedAt();
                } else if (time.isBefore(document.getUpdatedAt())) {
                    time = document.getUpdatedAt();
                }
            }
        }

        if (null != time && null != config.getStartTimestampFile()) {
            Files.write(Paths.get(config.getStartTimestampFile()), time.toString().getBytes());
        }
        return time;
    }

    public void stopGracefully() {
        this.shouldStopGracefully = true;
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING, "", null, null),
                "Publisher received graceful stop request. Processing batch now. When current batch is finished the publisher will exit. Please wait.");

    }

}
