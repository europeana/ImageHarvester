package eu.europeana.crfmigration.logic;

import com.mongodb.*;
import eu.europeana.crfmigration.domain.MongoConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Cleans a database. This means that all processed jobs will be marked as untouched and all statistics and metainfos will be deleted.
 */
public class Cleaner {

    private static final Logger LOG = LogManager.getLogger(MigrationManager.class.getName());

    private DB db;

    public Cleaner(MongoConfig config) throws IOException {
        final Mongo mongo = new Mongo(config.getHost(), config.getPort());

        if (!config.getdBUsername().equals("")) {
            db = mongo.getDB("admin");
            final Boolean auth = db.authenticate(config.getdBUsername(),
                    config.getdBPassword().toCharArray());
            if (!auth) {
                LOG.error("Mongo auth error");
                System.exit(-1);
            } else {
                LOG.info("Successful authentication");
            }
        }

        db = mongo.getDB(config.getdBName());
    }

    /**
     * The entry point of the cleaner
     */
    public void clean() {
        final DBCollection machineResourceReference = db.getCollection("MachineResourceReference");
        machineResourceReference.drop();
        LOG.info("Done with MachineResourceReference.");
        final DBCollection sourceDocumentProcessingStatistics = db.getCollection("SourceDocumentProcessingStatistics");
        sourceDocumentProcessingStatistics.drop();
        LOG.info("Done with SourceDocumentProcessingStatistics.");
        final DBCollection sourceDocumentReferenceMetaInfo = db.getCollection("SourceDocumentReferenceMetaInfo");
        sourceDocumentReferenceMetaInfo.drop();
        LOG.info("Done with SourceDocumentReferenceMetaInfo.");

        final DBCollection processingJob = db.getCollection("ProcessingJob");

        final DBObject clause1 = new BasicDBObject("state", "RUNNING");
        final DBObject clause2 = new BasicDBObject("state", "FINISHED");
        final BasicDBList or = new BasicDBList();
        or.add(clause1);
        or.add(clause2);
        final DBObject query = new BasicDBObject("$or", or);

        final BasicDBObject sortOrder = new BasicDBObject();
        sortOrder.put("$natural", 1);

        DBCursor jobCursor = processingJob.find(query).sort(sortOrder);
        jobCursor.addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        int i = 0;
        while (jobCursor.hasNext()) {
            try {
                final BasicDBObject oldJob = (BasicDBObject) jobCursor.next();
                processingJob.update(oldJob, new BasicDBObject("$set", new BasicDBObject("state", "READY")));

                i++;
                if (i > 100000) {
                    i = 0;
                    LOG.info("Done with another 100k processingJobs.");
                }
            } catch (Exception e) {
                jobCursor = processingJob.find(query).sort(sortOrder);
                jobCursor.skip(i - 1);
            }
        }
        LOG.info("Done with another 100k ProcessingJob.");
    }
}
