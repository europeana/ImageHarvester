package eu.europeana.harvester.performance;

import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.MorphiaDataStore;
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
 * Creates multiple jobs which contains a total of 500k of download tasks. Created for testing.
 */
public class Download_500k {

    private static final Logger LOG = LogManager.getLogger(Download_500k.class.getName());

    public static void main(String[] args) throws UnknownHostException {
        LOG.info("Starting download test...");

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

        final MorphiaDataStore datastore = new MorphiaDataStore(config.getString("mongo.host"),config.getInt("mongo.port"),config.getString("mongo.dbName"));
        final HarvesterClientImpl harvesterClient = new HarvesterClientImpl(datastore,new HarvesterClientConfig(WriteConcern.NONE));

        // ============================================================================================================

        final int nrOfLinks = 567000;
        final int linksPerJobs = 10001;
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
                        new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), null, line,
                                null, null, 0l, null, true);

                final GenericSubTaskConfiguration jobConfigs = new GenericSubTaskConfiguration(new ThumbnailConfig(50, 50));
                final List<ProcessingJobSubTask> subTaskList = new ArrayList<ProcessingJobSubTask>();
                subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.META_EXTRACTION, null));
                subTaskList.add(new ProcessingJobSubTask(ProcessingJobSubTaskType.GENERATE_THUMBNAIL, jobConfigs));

                final ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                        new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                reference.getId(), subTaskList);
                sourceDocumentReferences.add(reference);
                processingJobTaskDocumentReferences.add(processingJobTaskDocumentReference);
                i++;

                if(i == linksPerJobs) {
                    ProcessingJob processingJob =
                            new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"),
                                    processingJobTaskDocumentReferences, JobState.READY, "");
                    processingJobTaskDocumentReferences = new ArrayList<ProcessingJobTaskDocumentReference>();
                    processingJobs.add(processingJob);
                    i = 0;
                }
            }
            ProcessingJob processingJob =
                    new ProcessingJob(1, new Date(), new ReferenceOwner("1", "1", "1"),
                            processingJobTaskDocumentReferences, JobState.READY, "");
            processingJobs.add(processingJob);
            harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);

         //   harvesterClient.createOrModifyLinkCheckLimits(linkCheckLimits);
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
