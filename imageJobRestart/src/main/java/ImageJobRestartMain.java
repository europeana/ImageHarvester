import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.db.SourceDocumentReferenceMetaInfoDao;
import eu.europeana.harvester.db.mongo.ProcessingJobDaoImpl;
import eu.europeana.harvester.db.mongo.SourceDocumentReferenceMetaInfoDaoImpl;
import eu.europeana.harvester.domain.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.UnknownHostException;

public class ImageJobRestartMain {
    private static Logger LOG = LogManager.getLogger(ImageJobRestartMain.class.getName());
    private static PrintStream log;

    public static void main(String[] args) {
        final String configFilePath = "./extra-files/config-files/master.conf";
        final File configFile = new File(configFilePath);
        if (!configFile.exists()) {
            LOG.info("Config file not found!");
            System.exit(-1);
        }

        try {
            log = new PrintStream(new File("errors"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            log = System.out;
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        Datastore datastore = null;
        try {
            MongoClient mongo = new MongoClient(config.getString("mongo.host"), config.getInt("mongo.port"));
            Morphia morphia = new Morphia();
            String dbName = config.getString("mongo.dbName");

            if (!config.getString("mongo.username").equals("")) {
                final DB db = mongo.getDB("admin");
                final Boolean auth = db.authenticate(config.getString("mongo.username"),
                        config.getString("mongo.password").toCharArray());
                if (!auth) {
                    LOG.info("Mongo auth error");
                    System.out.println("Mongo auth error");
                    System.exit(-1);
                }
            }

            datastore = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            LOG.info(e.getMessage());
            System.exit(-1);
        }

        final SourceDocumentReferenceMetaInfoDao sourceDocumentReferenceMetaInfoDao =
                new SourceDocumentReferenceMetaInfoDaoImpl(datastore);
        final ProcessingJobDao processingJobDao = new ProcessingJobDaoImpl(datastore);

        //retrieve all
       final Query<SourceDocumentProcessingStatistics> query = datastore.find(SourceDocumentProcessingStatistics.class);

        if (null == query) {
            LOG.info("No SourceDocumentProcessingStatistics where found");
            System.out.println("No SourceDocumentProcessingStatistics where found");
            return;
        }



        long processedItems = 0;
        long imgMetaInfoItems = 0;

        LOG.info("stated updating all image related jobs");
        System.out.println("started updateing all image related jobs");
        // step 1. getting the document metainfo source document reference
        for (SourceDocumentProcessingStatistics procStat : query) {
            ++processedItems;
            if (0 == processedItems % 1000) {
                LOG.info("Processed " + processedItems + " items.........");
                LOG.info("Of which: " + imgMetaInfoItems + " had an imageMetaInfo.........");
                System.out.println(processedItems + " " + imgMetaInfoItems);
                log.println("Processed " + processedItems + " items.........");
                log.println("Of which: " + imgMetaInfoItems + " had an imageMetaInfo.........");
            }
            SourceDocumentReferenceMetaInfo docMetaInfoRef = sourceDocumentReferenceMetaInfoDao.read(procStat.getSourceDocumentReferenceId());

            if (null == docMetaInfoRef) {
                LOG.info("SourceDocumentReferenceMetaInfo not found for " + procStat.getId() + " (sourceDocRef " + procStat.getSourceDocumentReferenceId() + ")");
                log.println("SourceDocumentReferenceMetaInfo not found for " + procStat.getId() + " (sourceDocRef " + procStat.getSourceDocumentReferenceId() + ")");
                continue;
            }
            ImageMetaInfo imgMetaInfo = docMetaInfoRef.getImageMetaInfo();
            if (null == imgMetaInfo) {
                continue;
            }
            ++imgMetaInfoItems;


            /*
             * step 2.1 delete SourceDocumentReferenceMetaInfo
             */
            String error = sourceDocumentReferenceMetaInfoDao.delete(docMetaInfoRef.getId()).getError();
            if (null != error && !error.trim().equals("")) {
                LOG.error("Error occurred while trying to delete " + docMetaInfoRef.getId() + "\nError: " + error + "\n");
                log.println("Error occurred while trying to delete " + docMetaInfoRef.getId() + "\nError: " + error + "\n");
                log.println(imgMetaInfo);
                log.println(docMetaInfoRef);
            }

            /*
             * step 2.2 update job state
             */
            ProcessingJob job = processingJobDao.read(procStat.getProcessingJobId());
            final ProcessingJob newProcessingJob = job.withState(JobState.READY);
            if (!processingJobDao.update(newProcessingJob, WriteConcern.NONE)) {
                LOG.info("State for jobId: " + job.getId() + " not updated.");
                log.println("State for jobId: " + job.getId() + " not updated.");
                log.println(job);
            }
        }
        System.out.println("finished updateing all image related jobs");
        LOG.info("finished processing all image related jobs !");

    }
}
