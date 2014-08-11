package eu.europeana.harvester.performance;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import eu.europeana.harvester.utils.LinkParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Creates multiple jobs which contains a total of 50k of link check tasks. Created for testing.
 */
public class LinkCheck_50k {

    private static final Logger LOG = LogManager.getLogger(LinkCheck_50k.class.getName());

    public static void main(String[] args) {
        LOG.debug("Starting link check test...");

        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./extra-files/config-files/client.conf";
        } else {
            configFilePath = args[0];
        }

        File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config =
                ConfigFactory.parseFileAnySyntax(configFile,
                        ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));

        final Datastore datastore;
        Datastore datastore1;

        try {
            final MongoClient mongo = new MongoClient(config.getString("mongo.host"), config.getInt("mongo.port"));
            final Morphia morphia = new Morphia();
            final String dbName = config.getString("mongo.dbName");

            datastore1 = morphia.createDatastore(mongo, dbName);
        } catch (UnknownHostException e) {
            datastore1 = null;
            LOG.error(e.getMessage());
        }

        datastore = datastore1;

        final ProcessingJobDao processingJobDao = new ProcessingJobDaoImpl(datastore);
        final MachineResourceReferenceDao machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final LinkCheckLimitsDao linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);

        final HarvesterClientConfig harvesterClientConfig = new HarvesterClientConfig(WriteConcern.NONE);

        final HarvesterClientImpl harvesterClient = new HarvesterClientImpl(processingJobDao,
                machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao,
                linkCheckLimitsDao, harvesterClientConfig);

        // ============================================================================================================

        final int nrOfLinks = 54709;
        //final int nrOfLinks = 1500;
        final int linksPerJobs = 1001;
        final String outputFileName = "outLinkCheck";

        final LinkParser linkParser = new LinkParser(nrOfLinks, outputFileName);
        linkParser.start();

        final File links = new File("./harvester-client/src/main/resources/TestLinks/" + outputFileName);
        final List<ProcessingJob> processingJobs = new ArrayList<ProcessingJob>();
        try {
            final BufferedReader br = new BufferedReader(new FileReader(links));
            String line;
            final List<SourceDocumentReference> sourceDocumentReferences = new ArrayList<SourceDocumentReference>();
            List<ProcessingJobTaskDocumentReference> processingJobTaskDocumentReferences =
                    new ArrayList<ProcessingJobTaskDocumentReference>();

            int i = 0;

            while((line = br.readLine()) != null) {
                final SourceDocumentReference reference =
                        new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), line,
                                null, null, 0l, null);
                final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                        new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK, reference.getId());
                sourceDocumentReferences.add(reference);
                processingJobTaskDocumentReferences.add(processingJobTaskDocumentReference);
                i++;

                if(i == linksPerJobs) {
                    ProcessingJob processingJob =
                       new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"),
                               processingJobTaskDocumentReferences, JobState.READY);
                    processingJobTaskDocumentReferences = new ArrayList<ProcessingJobTaskDocumentReference>();
                    processingJobs.add(processingJob);
                    i = 0;
                }
            }
            ProcessingJob processingJob =
                    new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"),
                            processingJobTaskDocumentReferences, JobState.READY);
            processingJobs.add(processingJob);
            harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);

            final LinkCheckLimits linkCheckLimits = new LinkCheckLimits("1", 100*1024l, 100*1024l, 1000l, 500*1024l);
            harvesterClient.createOrModifyLinkCheckLimits(linkCheckLimits);
        } catch (FileNotFoundException e) {
            LOG.error(e.getMessage());
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }

        for(ProcessingJob processingJob : processingJobs) {
            harvesterClient.createProcessingJob(processingJob);
        }
    }
}
