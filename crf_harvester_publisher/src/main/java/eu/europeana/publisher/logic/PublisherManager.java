package eu.europeana.publisher.logic;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mongodb.DBCursor;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.publisher.dao.PublisherEuropeanaDao;
import eu.europeana.publisher.dao.PublisherHarvesterDao;
import eu.europeana.publisher.dao.SOLRWriter;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.HarvesterDocument;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logging.LoggingComponent;
import eu.europeana.publisher.logic.extract.FakeTagExtractor;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * It's responsible for the whole publishing process. It's the engine of the
 * publisher module.
 */
public class PublisherManager {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final PublisherConfig config;

    private final List<SOLRWriter> solrWriters;
    private PublisherEuropeanaDao publisherEuropeanaDao;
    private final List<PublisherHarvesterDao> publisherHarvesterDaos;

    private final PublisherMetrics publisherMetrics;

    public PublisherManager(PublisherConfig config) throws UnknownHostException {
        if (null == config) {
            throw new IllegalArgumentException("config cannot be null");
        }

        if (config.getSolrURL().isEmpty() || config.getTargetMongoConfig().isEmpty()) {
            throw new IllegalArgumentException("solr and target mongo configs cannot be empty");
        }

        if (config.getSolrURL().size() != config.getTargetMongoConfig().size()) {
            throw new IllegalArgumentException("solr and target mongo configs must be of the same size");
        }

        this.config = config;
        publisherMetrics = new PublisherMetrics();

        publisherEuropeanaDao = new PublisherEuropeanaDao(config.getSourceMongoConfig());

        publisherHarvesterDaos = new LinkedList<>();

        for (final MongoConfig target: config.getTargetMongoConfig()) {
            publisherHarvesterDaos.add(new PublisherHarvesterDao(target));
        }

        solrWriters = new LinkedList<>();

        for (final String url: config.getSolrURL()) {
            solrWriters.add(new SOLRWriter(url));
        }


        Slf4jReporter reporter = Slf4jReporter.forRegistry(PublisherMetrics.metricRegistry)
                .outputTo(org.slf4j.LoggerFactory.getLogger("metrics"))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).build();

        reporter.start(20, TimeUnit.SECONDS);

        if (null == config.getGraphiteConfig() || config.getGraphiteConfig().getServer().trim().isEmpty()) {
            return;
        }

        final InetSocketAddress addr = new InetSocketAddress(config.getGraphiteConfig().getServer(),
                config.getGraphiteConfig().getPort());
        Graphite graphite = new Graphite(addr);
        GraphiteReporter reporter2 = GraphiteReporter.forRegistry(PublisherMetrics.metricRegistry)
                .prefixedWith(config.getGraphiteConfig().getMasterId())
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS).filter(MetricFilter.ALL)
                .build(graphite);
        reporter2.start(20, TimeUnit.SECONDS);
    }

    public void start() throws IOException, SolrServerException, InterruptedException {
        try {
            publisherMetrics.startTotalTimer();
            startPublisher();
            publisherMetrics.stopTotalTimer();
        } finally {
            for (final Timer.Context context : publisherMetrics.getTimerContexts()) {
                context.close();
            }
        }
    }

    private List<HarvesterDocument> filterDocumentIds(List<HarvesterDocument> retreivedDocuments, final String publishingBatchId) {

        int size = -1;

        for (final SOLRWriter solrWriter: solrWriters) {
            retreivedDocuments = solrWriter.filterDocumentIds(retreivedDocuments, publishingBatchId);

            if (-1 == size) {
                size = retreivedDocuments.size();
            }
            else if (size != retreivedDocuments.size()) {
                LOG.error ("Solr at url "  + solrWriter.getSolrUrl() + " is inconsistent with other solrs");
                System.exit(-1);
            }
        }

        return retreivedDocuments;
    }

    private void startPublisher() throws SolrServerException, IOException, InterruptedException {
        DateTime currentTimestamp = config.getStartTimestamp();
        final DBCursor cursor = publisherEuropeanaDao.buildCursorForDocumentStatistics(currentTimestamp);
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                "Starting publishing process. The minimal timestamp is {}", currentTimestamp);

        while (cursor.hasNext()) {
            final String publishingBatchId = "publishing-batch-"+DateTime.now().getMillis()+"-"+Math.random();

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,publishingBatchId,null,null),
                    "Executing publishing CRF retrieval query {}", cursor.getQuery());

            List<HarvesterDocument> retrievedDocs = publisherEuropeanaDao.retrieveDocumentsWithMetaInfo(cursor, config.getBatch());

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,publishingBatchId,null,null),
                    "Retrieved CRF documents with meta info {}", retrievedDocs.size());

            if (null == retrievedDocs || retrievedDocs.isEmpty()) {
                LOG.error ("received null or empty documents from source mongo. Sleeping " + config.getSleepSecondsAfterEmptyBatch());
                TimeUnit.SECONDS.sleep(config.getSleepSecondsAfterEmptyBatch());
            }

            retrievedDocs = filterDocumentIds(retrievedDocs, publishingBatchId);

            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,publishingBatchId,null,null),
                    "Retrieved CRF documents after SOLR filtering {}", retrievedDocs.size());

            final List<CRFSolrDocument> solrDocuments = FakeTagExtractor.extractTags(retrievedDocs,publishingBatchId);


            if (null != solrDocuments && !solrDocuments.isEmpty() && updateDocuments(solrDocuments,publishingBatchId)) {
                writeMetaInfos(retrievedDocs);
            } else {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,publishingBatchId,null,null),
                        "There was a problem with writing this batch to solr. No metainfo was written to mongo. Maybe documents where empty ?");
            }

            try {
                currentTimestamp = updateTimestamp(currentTimestamp, retrievedDocs);
            } catch (IOException e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,publishingBatchId,null,null),
                        "Problem writing " + currentTimestamp + "to file: " + config.getStartTimestampFile(), e);
            }
            LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING,publishingBatchId,null,null),
                    "Updating timestamp after batch finished to "+currentTimestamp);
        }

        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                "Finished publishing all data");
    }

    private void writeMetaInfos (List<HarvesterDocument> retrievedDocs) {
        for (final PublisherHarvesterDao dao: publisherHarvesterDaos) {
            dao.writeMetaInfos(retrievedDocs);
        }
    }

    private boolean updateDocuments (List<CRFSolrDocument> solrDocuments, String publishingBatchId) throws IOException {
        for (final SOLRWriter solrWriter: solrWriters) {
            if (!solrWriter.updateDocuments(solrDocuments, publishingBatchId)) {
                LOG.error ("Failed to write documents to solr. BatchID " + publishingBatchId);
                System.exit(-1);
            }
        }

        return true;
    }

    private DateTime updateTimestamp(final DateTime currentTime, final Collection<HarvesterDocument> documents) throws
            IOException {
        DateTime time = currentTime;

        for (final HarvesterDocument document : documents) {
            if (null == time) {
                time = document.getUpdatedAt();
            } else if (time.isBefore(document.getUpdatedAt())) {
                time = document.getUpdatedAt();
            }
        }

        if (null != time && null != config.getStartTimestampFile()) {
            Files.write(Paths.get(config.getStartTimestampFile()), time.toString().getBytes());
        }
        return time;
    }
}
