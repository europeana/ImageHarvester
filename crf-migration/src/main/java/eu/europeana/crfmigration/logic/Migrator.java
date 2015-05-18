package eu.europeana.crfmigration.logic;

import com.mongodb.*;
import eu.europeana.crfmigration.domain.MongoConfig;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

/**
 * Creates the processing jobs and the source documents needed by them.
 */
public class Migrator {

    private static final Logger LOG = LogManager.getLogger(Migrator.class.getName());
    private final BufferedWriter bw;

    private DB db;
    private final HarvesterClientImpl harvesterClient;

    private final Date dateFilter;
    private final Map<String, String> records = new HashMap<String, String>();
    private final List<SourceDocumentReference> sourceDocumentReferences = new ArrayList<SourceDocumentReference>();
    private final List<ProcessingJob> processingJobs = new ArrayList<ProcessingJob>();

    final ExecutorService service = Executors.newSingleThreadExecutor();

    final Map<String, Long> linksPerIP = new HashMap<String, Long>();

    public Migrator(MongoConfig config, Date dateFilter) throws IOException {
        final Mongo mongo = new Mongo(config.getSourceHost(), config.getSourcePort());

        this.dateFilter = dateFilter;
        db = mongo.getDB("admin");
        final Boolean auth = db.authenticate(config.getSourceDBUsername(),
                config.getSourceDBPassword().toCharArray());
        if(!auth) {
            LOG.error("Mongo auth error");
            System.exit(-1);
        }

        db = mongo.getDB(config.getSourceDBName());
        final MorphiaDataStore datastore =
                new MorphiaDataStore(config.getTargetHost(), config.getTargetPort(),config.getTargetDBName());
        datastore.getMongo().getDB("admin").authenticate(config.getTargetDBUsername(),
                config.getTargetDBPassword().toCharArray());
        harvesterClient = new HarvesterClientImpl(datastore, new HarvesterClientConfig(WriteConcern.SAFE));

        final File file = new File("errors");
        if (!file.exists()) {
            file.createNewFile();
        }

        final FileWriter fw = new FileWriter(file.getAbsoluteFile());
        bw = new BufferedWriter(fw);
        bw.write("Errors during migration\n");
    }

    public void migrate() {
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
        while (recordCursor.hasNext()) {
            try {
                final BasicDBObject item = (BasicDBObject) recordCursor.next();
                final String about = (String) item.get("about");
                final BasicDBList collNames = (BasicDBList) item.get("europeanaCollectionName");
                final String collectionId = (String) collNames.get(0);
                records.put(about, collectionId);

                i++;
                if (i > 100000) {
                    i = 0;

                    try {
                        retrieveSourceDocumentReferences();
                    } catch(Exception e) {
                        LOG.info(e);
                    }
                    monitor();
                    LOG.info("Done with loading another 100k records.");
                }
            } catch(Exception e) {
                try {
                    bw.write("Error reading record after record: #" + i + "\n");
                } catch (IOException e1) {
                    e.printStackTrace();
                }
                recordCursor = recordCollection.find(filterByTimestampQuery, recordFields).sort(sortOrder);
                recordCursor.skip(i-1);
            }

        }

        retrieveSourceDocumentReferences();

        LOG.info("Done");
        monitor();
        service.shutdown();
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<ProcessingJobTaskDocumentReference> processingJobTaskDocumentReferences;

    private void retrieveSourceDocumentReferences() {
        for (final Object o : records.entrySet()) {
            processingJobTaskDocumentReferences = new ArrayList<ProcessingJobTaskDocumentReference>();
            try {
                final Map.Entry pairs = (Map.Entry) o;
                final ReferenceOwner referenceOwner = getReferenceOwner(pairs);

                final String aggregationAbout = "/aggregation/provider" + pairs.getKey();

                final DBObject aggregation = getAggregation(aggregationAbout);

                if (aggregation == null) {
                    bw.write("Missing aggregation: " + aggregationAbout + " at record: " + pairs.getKey() + "\n");
                    continue;
                }

                try {
                    Boolean thumbnail = false;
                    String thumbnailUrl = (String) aggregation.get("edmObject");
                    if(thumbnailUrl != null) {
                        thumbnail = true;
                    }

                    String ipAddress = "";

                    final String url = (String) aggregation.get("edmIsShownBy");
                    final BasicDBList resources = (BasicDBList) aggregation.get("hasView");

                    if (url != null) {
                        if(url.equals(thumbnailUrl)) {
                            ipAddress = addResourceForThumbnailingExtra(referenceOwner, url, URLSourceType.ISSHOWNBY);
                            thumbnailUrl = null;
                        } else {
                            ipAddress = addResourceForMetaExtraction(referenceOwner, url, URLSourceType.ISSHOWNBY);
                        }
                    }

                    if (resources != null) {
                        for(Object resource : resources) {
                            if(String.valueOf(resource).equals(url)) {
                                continue;
                            }
                            if(String.valueOf(resource).equals(thumbnailUrl)) {
                                ipAddress = addResourceForThumbnailingExtra(referenceOwner, String.valueOf(resource), URLSourceType.HASVIEW);
                                thumbnailUrl = null;
                            } else {
                                ipAddress = addResourceForMetaExtraction(referenceOwner, String.valueOf(resource), URLSourceType.HASVIEW);
                            }
                        }
                    }

                    if (thumbnailUrl != null) {
                        ipAddress = addResourceForThumbnailing(referenceOwner, thumbnailUrl, URLSourceType.ISSHOWNBY);
                    } else
                    if(!thumbnail) {
                        bw.write("No url for thumbnailing in record: " + pairs.getKey() + "\n");
                    }

                    if (url == null && resources == null && thumbnailUrl == null) {
                        bw.write("No resource in record: " + pairs.getKey() + "\n");
                    } else {
                        final ProcessingJob processingJob = new ProcessingJob(1, new Date(), referenceOwner,
                                        processingJobTaskDocumentReferences, JobState.READY, ipAddress);

                        processingJobs.add(processingJob);
                    }

                } catch (Exception e) {
                    bw.write("No resource in record: " + pairs.getKey() + "\n");
                }
            } catch (Exception e) {
                try {
                    bw.write("Error at record: " + ((Map.Entry) o).getKey() + "\n");
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        save();
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
        final DBCollection aggregationCollection = db.getCollection("Aggregation");

        final BasicDBObject whereQueryAggregation = new BasicDBObject();
        whereQueryAggregation.put("about", aggregationAbout);

        final BasicDBObject aggregationFields = new BasicDBObject();
        aggregationFields.put("edmObject", 1);
        aggregationFields.put("edmIsShownBy", 1);
        aggregationFields.put("hasView", 1);
        aggregationFields.put("_id", 0);

        return aggregationCollection.findOne(whereQueryAggregation, aggregationFields);
    }

    private String addResourceForMetaExtraction(ReferenceOwner referenceOwner, String url, URLSourceType urlSourceType) {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(referenceOwner, urlSourceType, url, null, null, 0l, null, true);
        sourceDocumentReferences.add(sourceDocumentReference);

        final List<ProcessingJobSubTask> subTaskList = new ArrayList<ProcessingJobSubTask>();
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null));

        final ProcessingJobTaskDocumentReference metainfoTask =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        sourceDocumentReference.getId(), subTaskList);
        processingJobTaskDocumentReferences.add(metainfoTask);

        return getIPAddress(url);
    }

    private String addResourceForThumbnailing(ReferenceOwner referenceOwner, String url, URLSourceType urlSourceType) {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(referenceOwner, urlSourceType, url, null, null, 0l, null, true);
        sourceDocumentReferences.add(sourceDocumentReference);

        final GenericSubTaskConfiguration subTaskConfiguration1 =
                new GenericSubTaskConfiguration(new ThumbnailConfig(180, 180));
        final GenericSubTaskConfiguration subTaskConfiguration2 =
                new GenericSubTaskConfiguration(new ThumbnailConfig(200, 200));

        final List<ProcessingJobSubTask> subTaskList = new ArrayList<ProcessingJobSubTask>();
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, subTaskConfiguration1));
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, subTaskConfiguration2));
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.COLOR_EXTRACTION, null));

        final ProcessingJobTaskDocumentReference thumbnailTask =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        sourceDocumentReference.getId(), subTaskList);
        processingJobTaskDocumentReferences.add(thumbnailTask);

        return getIPAddress(url);
    }

    private String addResourceForThumbnailingExtra(ReferenceOwner referenceOwner, String url, URLSourceType urlSourceType) {
        final SourceDocumentReference sourceDocumentReference =
                new SourceDocumentReference(referenceOwner, urlSourceType, url, null, null, 0l, null, true);
        sourceDocumentReferences.add(sourceDocumentReference);

        final GenericSubTaskConfiguration subTaskConfiguration1 =
                new GenericSubTaskConfiguration(new ThumbnailConfig(180, 180));
        final GenericSubTaskConfiguration subTaskConfiguration2 =
                new GenericSubTaskConfiguration(new ThumbnailConfig(200, 200));

        final List<ProcessingJobSubTask> subTaskList = new ArrayList<ProcessingJobSubTask>();
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, subTaskConfiguration1));
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, subTaskConfiguration2));
        subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null));

        final ProcessingJobTaskDocumentReference thumbnailTask =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        sourceDocumentReference.getId(), subTaskList);
        processingJobTaskDocumentReferences.add(thumbnailTask);

        return getIPAddress(url);
    }

    private void save() {
        records.clear();
        saveSourceDocumentReferences();
        sourceDocumentReferences.clear();
        saveProcessingJobs();
        processingJobs.clear();
    }

    private void saveSourceDocumentReferences() {
        harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);
    }

    private void saveProcessingJobs() {
        for(final ProcessingJob processingJob : processingJobs) {
            harvesterClient.createProcessingJob(processingJob);
        }
    }

    /**
     * Gets the ip address of a source document.
     * @param url the resource url
     * @return - ip address
     */
    private String getIPAddress(final String url) {
        String ipAddress;
        final Future<String> future = service.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                final InetAddress address = InetAddress.getByName(new URL(url).getHost());

                return address.getHostAddress();
            }
        });

        try {
            ipAddress = future.get(60000, TimeUnit.MILLISECONDS);
        }
        catch(TimeoutException e) {
            ipAddress = null;
        } catch (InterruptedException e) {
            ipAddress = null;
        } catch (ExecutionException e) {
            ipAddress = null;
        }

        if(linksPerIP.containsKey(ipAddress)) {
            final Long nr = linksPerIP.get(ipAddress);
            linksPerIP.put(ipAddress, nr + 1);
        } else {
            linksPerIP.put(ipAddress, 1l);
        }

        return ipAddress;
    }

    private void monitor() {
        LOG.info("========================================================");
        LOG.info("#IP: {}", linksPerIP.size());
        for(Map.Entry entry : linksPerIP.entrySet()) {
            LOG.info("{} : {}", entry.getKey(), entry.getValue());
        }
        LOG.info("========================================================");
    }

}
