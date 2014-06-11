package eu.europeana.harvester.client;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

class ClientMain {

    public static void main(String[] args) {
        System.out.println("Starting demo...");

        String configFilePath;

        if(args.length == 0) {
            configFilePath = "./src/main/resources/client.conf";
            //configFilePath = "./client.conf";
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

        HarvesterClientImpl harvesterClient = new HarvesterClientImpl(processingJobDao, machineResourceReferenceDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, linkCheckLimitsDao);

        //========================================================================================================

        MachineResourceReference machineResourceReference =
                new MachineResourceReference("129.164.179.27", null, null, 567*1024l, 5l);
        harvesterClient.createOrModifyProcessingLimits(machineResourceReference);

        machineResourceReference = new MachineResourceReference("157.166.249.13", null, null, 1024*1024l, 2l);
        harvesterClient.createOrModifyProcessingLimits(machineResourceReference);

        machineResourceReference = new MachineResourceReference("108.171.213.134", null, null, 512*1024l, 3l);
        harvesterClient.createOrModifyProcessingLimits(machineResourceReference);

        machineResourceReference = new MachineResourceReference("74.220.207.111", null, null, 512*1024l, 3l);
        harvesterClient.createOrModifyProcessingLimits(machineResourceReference);

        SourceDocumentReference cnn =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"), "http://edition.cnn.com",
                        null, null, 0l, null);
        ProcessingJobTaskDocumentReference cnnJob =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, cnn.getId());

        SourceDocumentReference pic1 =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"),
                        "http://jwst.nasa.gov/images3/flightmirrorarrive1.jpg", null, null, 0l, null);
        ProcessingJobTaskDocumentReference pic1Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CONDITIONAL_DOWNLOAD, pic1.getId());

        SourceDocumentReference pic2 =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"),
                        "http://jwst.nasa.gov/images3/flightmirrorarrive2.jpg", null, null, 0l, null);
        ProcessingJobTaskDocumentReference pic2Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, pic2.getId());

        SourceDocumentReference pic3 =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"),
                        "http://jwst.nasa.gov/images3/flightmirrorarrive3.jpg", null, null, 0l, null);
        ProcessingJobTaskDocumentReference pic3Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, pic3.getId());

        SourceDocumentReference pic4 =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"),
                        "http://jwst.nasa.gov/images3/flightmirrorarrive4.jpg", null, null, 0l, null);
        ProcessingJobTaskDocumentReference pic4Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, pic4.getId());

        SourceDocumentReference pic5 =
                new SourceDocumentReference(new ReferenceOwner("1", "1", "1"),
                        "http://jwst.nasa.gov/images3/flightmirrorarrive5.jpg", null, null, 0l, null);
        ProcessingJobTaskDocumentReference pic5Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, pic5.getId());


        SourceDocumentReference file1 =
                new SourceDocumentReference(new ReferenceOwner("1", "2", "1"),
                        "http://www.wswd.net/testdownloadfiles/5MB.zip", null, null, 0l, null);
        ProcessingJobTaskDocumentReference file1Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, file1.getId());

        SourceDocumentReference file2 =
                new SourceDocumentReference(new ReferenceOwner("1", "2", "1"),
                        "http://www.wswd.net/testdownloadfiles/10MB.zip", null, null, 0l, null);
        ProcessingJobTaskDocumentReference file2Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD, file2.getId());

        SourceDocumentReference file3 =
                new SourceDocumentReference(new ReferenceOwner("1", "2", "1"),
                        "http://www.wswd.net/testdownloadfiles/20MB.zip", null, null, 0l, null);
        ProcessingJobTaskDocumentReference file3Job =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.CHECK_LINK, file3.getId());


        SourceDocumentReference redirect = new SourceDocumentReference(new ReferenceOwner("1", "3", "1"),
                "http://www.filipel.ro", null, null, 0l, null);
        ProcessingJobTaskDocumentReference redirectJob =
                new ProcessingJobTaskDocumentReference(DocumentReferenceTaskType.UNCONDITIONAL_DOWNLOAD,
                        redirect.getId());

        List<SourceDocumentReference> sourceDocumentReferences = new ArrayList<SourceDocumentReference>();
        sourceDocumentReferences.add(cnn);
        sourceDocumentReferences.add(pic1);
        sourceDocumentReferences.add(pic2);
        sourceDocumentReferences.add(pic3);
        sourceDocumentReferences.add(pic4);
        sourceDocumentReferences.add(pic5);
        sourceDocumentReferences.add(file1);
        sourceDocumentReferences.add(file2);
        sourceDocumentReferences.add(file3);
        sourceDocumentReferences.add(redirect);

        harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);

        List<ProcessingJobTaskDocumentReference> task1 =
                Arrays.asList(cnnJob, pic1Job, pic2Job, pic3Job, pic4Job, pic5Job);
        List<ProcessingJobTaskDocumentReference> task2 = Arrays.asList(file1Job, file2Job, file3Job);
        List<ProcessingJobTaskDocumentReference> task3 = Arrays.asList(redirectJob);

        ProcessingJob processingJob1 =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "1", "1"), task1, JobState.READY);
        ProcessingJob processingJob2 =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "2", "1"), task2, JobState.READY);
        ProcessingJob processingJob3 =
                new ProcessingJob(new Date(), new ReferenceOwner("1", "3", "1"), task3, JobState.READY);

        //harvesterClient.createProcessingJob(processingJob1);
        harvesterClient.createProcessingJob(processingJob2);
        harvesterClient.createProcessingJob(processingJob3);

        LinkCheckLimits linkCheckLimits = new LinkCheckLimits("1", 100*1024l, 100*1024l, 0l, 500*1024l);
        harvesterClient.createOrModifyLinkCheckLimits(linkCheckLimits);
    }
}
