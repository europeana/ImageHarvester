package eu.europeana.crfmigration.logic;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.mongodb.*;
import eu.europeana.JobCreator.JobCreator;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.maven.shared.utils.StringUtils;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Creates the processing jobs and the source documents needed by them.
 */
public class Migrator {

    private static final Logger LOG = LogManager.getLogger(Migrator.class.getName());

    private DB db;
    private final HarvesterClientImpl harvesterClient;

    private final Date dateFilter;

    private final JobCreator jobCreator = new JobCreator();
    private final MigratorMetrics metrics = new MigratorMetrics();

    private final int batch;

    public Migrator(MongoConfig config, Date dateFilter) throws IOException {
        final Mongo mongo = new Mongo(config.getSourceHost(), config.getSourcePort());

        this.dateFilter = dateFilter;

        this.batch = config.getBatch();

        if (StringUtils.isNotEmpty(config.getSourceDBUsername()) && StringUtils.isNotEmpty(config.getSourceDBPassword())) {
            db = mongo.getDB("admin");
            final Boolean auth = db.authenticate(config.getSourceDBUsername(),
                                                 config.getSourceDBPassword().toCharArray());
            if (!auth) {
                LOG.error("Mongo source auth error");
                System.exit(-1);
            }
        }

        db = mongo.getDB(config.getSourceDBName());
        final MorphiaDataStore datastore =
                new MorphiaDataStore(config.getTargetHost(), config.getTargetPort(),config.getTargetDBName());


        if (StringUtils.isNotEmpty(config.getTargetDBUsername())  &&
            StringUtils.isNotEmpty(config.getTargetDBPassword())) {

            final Boolean auth = datastore.getMongo().getDB("admin").authenticate(config.getTargetDBUsername(),
                                                                                  config.getTargetDBPassword()
                                                                                        .toCharArray());


            if (!auth) {
                LOG.error("Mongo target auth error");
                System.exit(-1);
            }
        }

        harvesterClient = new HarvesterClientImpl(datastore, new HarvesterClientConfig(WriteConcern.SAFE));

        Slf4jReporter reporter = Slf4jReporter.forRegistry(MigratorMetrics.metricRegistry)
                                              .outputTo(org.slf4j.LoggerFactory.getLogger(LOG.getName()))
                                              .convertRatesTo(TimeUnit.SECONDS)
                                              .convertDurationsTo(TimeUnit.MILLISECONDS)
                                              .build();

        reporter.start(1, TimeUnit.MINUTES);

        if (null == config.getGraphiteServer() || config.getGraphiteServer().trim().isEmpty()) {
            return;
        }

        Graphite graphite = new Graphite(new InetSocketAddress(config.getGraphiteServer(),
                                                               config.getGraphitePort()
        )
        );
        GraphiteReporter reporter2 = GraphiteReporter.forRegistry(MigratorMetrics.metricRegistry)
                                                     .prefixedWith(config.getGraphiteMasterId())
                                                     .convertRatesTo(TimeUnit.SECONDS)
                                                     .convertDurationsTo(TimeUnit.MILLISECONDS)
                                                     .filter(MetricFilter.ALL)
                                                     .build(graphite);
        reporter2.start(30, TimeUnit.SECONDS);
    }

    public void migrate() {
        try {
            metrics.startTotalMigrationTimer();
            starMigration();
        }
        finally {
            for (final Timer.Context context: metrics.getAllTimeContexts()) {
                context.stop();
            }
        }
    }

    private void starMigration() {
        LOG.info("start migration");
        final DBCollection recordCollection = db.getCollection("record");
         DBObject filterByTimestampQuery = new BasicDBObject();
        final BasicDBObject recordFields = new BasicDBObject();

        if (null != dateFilter) {
            filterByTimestampQuery = QueryBuilder.start().put("timestampUpdated").greaterThanEquals(dateFilter).get();
            LOG.info("Query: " + filterByTimestampQuery);
        }

        recordFields.put("about", 1);
        recordFields.put("", 1);
        recordFields.put("timestampUpdated", 1);
        recordFields.put("europeanaCollectionName", 1);
        recordFields.put("_id", 0);
        final BasicDBObject sortOrder = new BasicDBObject();
        sortOrder.put("$natural", 1);

        int i = 0;

        DBCursor recordCursor = recordCollection.find(filterByTimestampQuery, recordFields).sort(sortOrder);
        recordCursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);
        final Map<String, String> records = new HashMap<>();
        metrics.logBatchSize(batch);
        while (recordCursor.hasNext()) {
            try {
                final BasicDBObject item = (BasicDBObject) recordCursor.next();
                final String about = (String) item.get("about");
                final BasicDBList collNames = (BasicDBList) item.get("europeanaCollectionName");
                final String collectionId = (String) collNames.get(0);
                records.put(about, collectionId);

                i++;
                if (i > batch) {
                    i = 0;
                    metrics.logBatchDocumentProcessing(i);

                    try {
                        metrics.startSourceDocumentReferencesTimer();
                        retrieveSourceDocumentReferences(records);
                        metrics.stopSourceDocumentReferencesTimer();
                    } catch(Exception e) {
                        metrics.incBatchProcessingError();
                        LOG.info(e);
                    }
                    records.clear();
                    LOG.error("Done with loading another " + batch + " records.");
                }
            } catch(Exception e) {
                LOG.error ("Error reading record after reacord: #" + i + "\n");
                metrics.incErrorReadingRecord();

                recordCursor = recordCollection.find(filterByTimestampQuery, recordFields).sort(sortOrder);
                recordCursor.skip(i-1);
            }
        }

        metrics.startSourceDocumentReferencesTimer();
        retrieveSourceDocumentReferences(records);
        metrics.stopSourceDocumentReferencesTimer();

        LOG.info("Done");
    }



    private void retrieveSourceDocumentReferences(final Map<String, String> records) {
        LOG.info ("creating jobs");
        final List<ProcessingJob> jobs = new ArrayList<>();
        final List<SourceDocumentReference> sourceDocumentReferences = new ArrayList<>();
        for (final Map.Entry<String, String> record: records.entrySet()) {
            try {
                final ReferenceOwner referenceOwner = getReferenceOwner(record);

                final DBObject aggregation = getAggregation("/aggregation/provider" + record.getKey());

                if (null == aggregation) {
                    LOG.error("Missing aggregation: /aggregation/provider" + record.getKey() + " at record: " + record.getKey() + "\n");
                    metrics.incEmptyAggregations();
                    continue;
                }

                final String edmObject = (String) aggregation.get("edmObject");
                final BasicDBList hasViews = (BasicDBList) aggregation.get("hasView");
                final String edmIsShownBy = (String) aggregation.get("edmIsShownBy");
                final String edmIsShownAt = (String) aggregation.get("edmIsShownAt");


                final List<String> edmHasViews = null == hasViews ?
                                                            null :
                                                            Arrays.asList(hasViews.toArray(new String[hasViews.size()]));

                sourceDocumentReferences.addAll(createSourceDocumentReferences(referenceOwner, edmObject, edmHasViews,
                                                                               edmIsShownBy, edmIsShownAt
                                                                              )
                                               );

                try {
                    jobs.addAll(jobCreator.createJobs(referenceOwner.getCollectionId(), referenceOwner.getProviderId(),
                                                      referenceOwner.getRecordId(), edmObject, edmHasViews,
                                                      edmIsShownBy, edmIsShownAt, null));

                } catch (MalformedURLException | UnknownHostException e) {
                    LOG.error(e);
                    metrics.incInvalidUrl();
                }
            }
            catch (Exception e) {
                LOG.error ("Exception caught during record processing: " + e.getMessage(), e);
                metrics.incProcessingError();
            }
        }

        metrics.logNumberOfSourceDocumentReferencesPerBatch(sourceDocumentReferences.size());
        metrics.logNumberOfProcessingJobsPerBatch(jobs.size());

        LOG.info("done creating jobs");

        saveSourceDocumentReferences(sourceDocumentReferences);
        saveProcessingJobs(jobs);
    }

    private List<SourceDocumentReference> createSourceDocumentReferences (ReferenceOwner referenceOwner, String edmObject,
                                                                          List<String> edmHasViews, String edmIsShownBy,
                                                                          String edmIsShownAt
                                                                         ) {
        final List<SourceDocumentReference> references = new ArrayList<>();

        if (null != edmObject) {
            references.add(new SourceDocumentReference(referenceOwner, URLSourceType.ISSHOWNBY, edmObject,
                                                       null, null, 0L, null, true
                                                     )
                          );
        }

        if (null != edmHasViews) {
            for (final String url: edmHasViews) {
                references.add(new SourceDocumentReference(referenceOwner, URLSourceType.HASVIEW, url,
                                                           null, null, 0L, null, true)
                              );
            }

        }

        if (null != edmIsShownBy) {
            references.add(new SourceDocumentReference(referenceOwner, URLSourceType.ISSHOWNBY, edmIsShownBy,
                                                       null, null, 0L, null, true
                                                      )
                          );

        }

        if (null != edmIsShownAt) {
            references.add(new SourceDocumentReference(referenceOwner, null, edmIsShownAt, null, null, 0L, null, true));

        }
        return references;
    }

    private ReferenceOwner getReferenceOwner(final Map.Entry pairs) {
        final String about = (String) pairs.getKey();
        final String collectionId = (String) pairs.getValue();

        final String[] temp = about.split("/");
        final String providerId = temp[1];
        final String recordId = about;

        return new ReferenceOwner(providerId, collectionId, recordId);
    }

    private DBObject getAggregation(String aggregationAbout) {
        try {
            metrics.startGetAggregationTimer();

            final DBCollection aggregationCollection = db.getCollection("Aggregation");

            final BasicDBObject whereQueryAggregation = new BasicDBObject();
            whereQueryAggregation.put("about", aggregationAbout);

            final BasicDBObject aggregationFields = new BasicDBObject();
            aggregationFields.put("edmObject", 1);
            aggregationFields.put("edmIsShownBy", 1);
            aggregationFields.put("edmIsShownAt", 1);
            aggregationFields.put("hasView", 1);
            aggregationFields.put("_id", 0);

            return aggregationCollection.findOne(whereQueryAggregation, aggregationFields);
        }
        finally {
            metrics.stopGetAggregationTimer();
        }
    }

    private void saveSourceDocumentReferences(final List<SourceDocumentReference> sourceDocumentReferences) {
        try{
            LOG.info ("start saving sourceDocumentReferences");
            metrics.startSaveSourceDocumentReferencesTimer();
            harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);
        }
        finally {
            metrics.stopSaveSourceDocumentReferencesTimer();
            LOG.info ("saving sourceDocumentReferences is done");
        }
    }

    private void saveProcessingJobs(final List<ProcessingJob> jobs) {
        LOG.info ("saving created jobs");
        try {
            metrics.startSaveProcessingJobsTimer();
            for (final ProcessingJob processingJob : jobs) {
                harvesterClient.createProcessingJob(processingJob);
            }
        }
        finally {
            metrics.stopSaveProcessingJobsTimer();
            LOG.info ("done saving jobs");
        }
    }
}
