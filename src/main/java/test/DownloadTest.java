package test;

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

import java.io.*;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DownloadTest {

    public static void main(String[] args) {
        System.out.println("Starting download test...");

        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./src/main/resources/client.conf";
        } else {
            configFilePath = args[0];
        }

        File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            System.out.println("Config file not found!");
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
            e.printStackTrace();
        }

        datastore = datastore1;

        final ProcessingJobDao processingJobDao = new ProcessingJobDaoImpl(datastore);
        final MachineResourceReferenceDao machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final LinkCheckLimitsDao linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);

        final HarvesterClientConfig harvesterClientConfig = new HarvesterClientConfig(WriteConcern.NONE);

        HarvesterClientImpl harvesterClient = new HarvesterClientImpl(processingJobDao, machineResourceReferenceDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, linkCheckLimitsDao, harvesterClientConfig);

        // ======================================================

        final int nrOfLinks = 100;
        final int linksPerJobs = 200;
        final String outputFileName = "outDownload";

        LinkParser linkParser = new LinkParser(nrOfLinks, outputFileName);
        linkParser.start();

        File links = new File("./src/main/resources/TestLinks/" + outputFileName);
        List<ProcessingJob> processingJobs = new ArrayList<ProcessingJob>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(links));
            String line = "";
            List<SourceDocumentReference> sourceDocumentReferences = new ArrayList<SourceDocumentReference>();
            List<ProcessingJobTaskDocumentReference> processingJobTaskDocumentReferences =
                    new ArrayList<ProcessingJobTaskDocumentReference>();
            int i = 0;

            while((line = br.readLine()) != null) {
                SourceDocumentReference reference =
                        new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), line,
                                null, null, 0l, null);
                ProcessingJobTaskDocumentReference processingJobTaskDocumentReference =
                        new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                                reference.getId());
                sourceDocumentReferences.add(reference);
                processingJobTaskDocumentReferences.add(processingJobTaskDocumentReference);
                i++;

                if(i == linksPerJobs) {
                    ProcessingJob processingJob =
                            new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"),
                                    processingJobTaskDocumentReferences, JobState.READY);
                    processingJobTaskDocumentReferences = new ArrayList<ProcessingJobTaskDocumentReference>();
                    processingJobs.add(processingJob);
                    i = 0;
                }
            }

            ProcessingJob processingJob =
                    new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"),
                            processingJobTaskDocumentReferences, JobState.READY);
            processingJobs.add(processingJob);
            harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);

            LinkCheckLimits linkCheckLimits = new LinkCheckLimits("1", 100*1024l, 100*1024l, 1000l, 500*1024l);
            harvesterClient.createOrModifyLinkCheckLimits(linkCheckLimits);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        for(ProcessingJob processingJob : processingJobs) {
            harvesterClient.createProcessingJob(processingJob);
        }
    }
}
