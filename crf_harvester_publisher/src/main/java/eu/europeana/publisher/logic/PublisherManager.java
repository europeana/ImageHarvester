package eu.europeana.publisher.logic;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.google.common.collect.Lists;
import com.mongodb.*;
import eu.europeana.publisher.dao.PublisherEuropeanaDao;
import eu.europeana.publisher.dao.PublisherHarvesterDAO;
import eu.europeana.publisher.domain.RetrievedDocument;
import eu.europeana.publisher.dao.SOLRWriter;
import eu.europeana.publisher.domain.CRFSolrDocument;
import eu.europeana.publisher.domain.PublisherConfig;
import eu.europeana.publisher.logic.extractor.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.joda.time.DateTime;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * It's responsible for the whole publishing process. It's the engine of the
 * publisher module.
 */
public class PublisherManager {
    private static final Logger LOG = LogManager.getLogger(PublisherManager.class.getName());

    private final PublisherConfig config;


    private final SOLRWriter solrWriter;
    private PublisherEuropeanaDao publisherEuropeanaDao;
    private PublisherHarvesterDAO publisherHarvesterDAO;

    private final PublisherMetrics publisherMetrics;

    public PublisherManager (PublisherConfig config) throws UnknownHostException {
        this.config = config;
        publisherMetrics = new PublisherMetrics();

        publisherEuropeanaDao = new PublisherEuropeanaDao(config.getSourceMongoConfig(), publisherMetrics);
        publisherHarvesterDAO = new PublisherHarvesterDAO(config.getTargetMongoConfig(), publisherMetrics);


        solrWriter = new SOLRWriter(config.getSolrURL());

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

    public void start () throws IOException, SolrServerException {
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

    private void startPublisher() throws SolrServerException, IOException {
        DateTime currentTimestamp = config.getStartTimestamp();
        final DBCursor cursor = publisherEuropeanaDao.buildCursorForDocumentStatistics(config.getStartTimestamp().toDate());
        while (true) {

            final Map<String, RetrievedDocument> retrievedDocs = publisherEuropeanaDao.retrieveDocumentsWithMetaInfo(cursor,
                                                                                                                     config.getBatch());

            if (null == retrievedDocs || retrievedDocs.isEmpty()) {
                break;
            }

            retrievedDocs.keySet().removeAll(solrWriter.filterDocumentIds(Lists.newArrayList(retrievedDocs.keySet())));


            final List<CRFSolrDocument> solrDocuments = FakeTagExtractor.extractTags(retrievedDocs.values());


            if (null != solrDocuments && !solrDocuments.isEmpty() && solrWriter.updateDocuments(solrDocuments)) {
                publisherHarvesterDAO.writeMetaInfos(retrievedDocs.values());
            }

            if (null != config.getStartTimestampFile()) {
                try {
                    currentTimestamp = updateTimestamp(currentTimestamp, retrievedDocs.values());
                } catch (IOException e) {
                    LOG.error("Problem writing " + currentTimestamp + "to file: " + config.getStartTimestampFile(), e);
                }
                LOG.info("New successful timestamp: " + currentTimestamp);
            }
        }
    }

    private DateTime updateTimestamp(final DateTime currentTime, final Collection<RetrievedDocument> documents) throws
                                                                                                                IOException {
        DateTime time = currentTime;

        for (final RetrievedDocument document: documents) {
            if (null == time) {
                time = document.getDocumentStatistic().getUpdatedAt();
            }
            else if (time.isBefore(document.getDocumentStatistic().getUpdatedAt())) {
                time =document.getDocumentStatistic().getUpdatedAt();
            }
        }

        if (null != time) {
            Files.write(Paths.get(config.getStartTimestampFile()), time.toString().getBytes());
        }
        return time;
    }
}
