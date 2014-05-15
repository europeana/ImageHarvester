package eu.europeana.harvester.client;

import com.mongodb.MongoClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.db.*;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.harvester.domain.*;
import org.joda.time.Duration;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

class ClientMain {

    public static void main(String[] args) {
        System.out.println("Starting Demo...");

        String configFilePath;

        if(args.length == 0) {
            configFilePath = "client";
        } else {

            configFilePath = args[0];
        }

        final Config config = ConfigFactory.load(configFilePath);

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
        final ProcessingLimitsDao processingLimitsDao = new ProcessingLimitsDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final LinkCheckLimitsDao linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);

        HarvesterClientImpl harvesterClient = new HarvesterClientImpl(processingJobDao, processingLimitsDao,
                sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao, linkCheckLimitsDao);

        //======================================

        ProcessingLimits processingLimits = new ProcessingLimits(1l, 567*1024l, 5l);
        harvesterClient.createOrModifyProcessingLimits(processingLimits);

        processingLimits = new ProcessingLimits(processingLimits.getId(), 1l, 1024*1024l, 2l);
        harvesterClient.createOrModifyProcessingLimits(processingLimits);

        processingLimits = new ProcessingLimits(2l, 512*1024l, 3l);
        harvesterClient.createOrModifyProcessingLimits(processingLimits);

        SourceDocumentReference cnn =
                new SourceDocumentReference(1l, 1l, 1l, "http://edition.cnn.com", false, true, false);
        SourceDocumentReference pic1 =
                new SourceDocumentReference(1l, 1l, 1l,
                        "http://jwst.nasa.gov/images3/flightmirrorarrive1.jpg", false, true, false);
        SourceDocumentReference pic2 =
                new SourceDocumentReference(1l, 1l, 1l,
                        "http://jwst.nasa.gov/images3/flightmirrorarrive2.jpg", false, true, false);
        SourceDocumentReference pic3 =
                new SourceDocumentReference(1l, 1l, 1l,
                        "http://jwst.nasa.gov/images3/flightmirrorarrive3.jpg", false, true, false);
        SourceDocumentReference pic4 =
                new SourceDocumentReference(1l, 1l, 1l,
                        "http://jwst.nasa.gov/images3/flightmirrorarrive4.jpg", false, true, false);
        SourceDocumentReference pic5 =
                new SourceDocumentReference(1l, 1l, 1l,
                        "http://jwst.nasa.gov/images3/flightmirrorarrive5.jpg", false, true, false);

        SourceDocumentReference file1 =
                new SourceDocumentReference(1l, 2l, 1l,
                        "http://download.thinkbroadband.com/5MB.zip", false, true, false);
        SourceDocumentReference file2 =
                new SourceDocumentReference(1l, 2l, 1l,
                        "http://download.thinkbroadband.com/10MB.zip", false, true, false);

        harvesterClient.createOrModifySourceDocumentReference(cnn);
        harvesterClient.createOrModifySourceDocumentReference(pic1);
        harvesterClient.createOrModifySourceDocumentReference(pic2);
        harvesterClient.createOrModifySourceDocumentReference(pic3);
        harvesterClient.createOrModifySourceDocumentReference(pic4);
        harvesterClient.createOrModifySourceDocumentReference(pic5);
        harvesterClient.createOrModifySourceDocumentReference(file1);
        harvesterClient.createOrModifySourceDocumentReference(file2);

        List<String> urls1 =
                Arrays.asList(cnn.getId(), pic1.getId(), pic2.getId(), pic3.getId(), pic4.getId(), pic5.getId());
        List<String> urls2 = Arrays.asList(file1.getId(), file2.getId());
        ProcessingJob processingJob1 = new ProcessingJob(new Date(), 1l, 1l, 1l, urls1, JobState.READY);
        ProcessingJob processingJob2 = new ProcessingJob(new Date(), 1l, 2l, 1l, urls2, JobState.READY);

        harvesterClient.createProcessingJob(processingJob1);
        harvesterClient.createProcessingJob(processingJob2);

        LinkCheckLimits linkCheckLimits = new LinkCheckLimits(100*1024l, 100*1024l, Duration.ZERO, 500*1024l);
        harvesterClient.createOrModifyLinkCheckLimits(linkCheckLimits);
    }
}
