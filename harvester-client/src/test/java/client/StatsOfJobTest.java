package client;

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
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.ProcessingJobStats;
import eu.europeana.harvester.domain.ProcessingState;
import junit.framework.TestCase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

public class StatsOfJobTest extends TestCase {

    private static final Logger LOG = LogManager.getLogger(StatsOfJobTest.class.getName());

    private HarvesterClientImpl harvesterClient;

    private ProcessingJobDao processingJobDao;

    public void setUp() throws Exception {
        super.setUp();

        String configFilePath = "./extra-files/config-files/client.conf";

        File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error("Config file not found!");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
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

        processingJobDao = new ProcessingJobDaoImpl(datastore);
        final MachineResourceReferenceDao machineResourceReferenceDao = new MachineResourceReferenceDaoImpl(datastore);
        final SourceDocumentReferenceDao sourceDocumentReferenceDao = new SourceDocumentReferenceDaoImpl(datastore);
        final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao =
                new SourceDocumentProcessingStatisticsDaoImpl(datastore);
        final LinkCheckLimitsDao linkCheckLimitsDao = new LinkCheckLimitsDaoImpl(datastore);

        final HarvesterClientConfig harvesterClientConfig = new HarvesterClientConfig(WriteConcern.NONE);

        harvesterClient = new HarvesterClientImpl(processingJobDao,
                machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao, sourceDocumentReferenceDao,
                sourceDocumentReferenceMetaInfoDao, linkCheckLimitsDao, harvesterClientConfig);
    }

    public void testStatsOfJob() throws Exception {
        final String jobID = "b8649990-8e33-43da-adcb-4964c16957f7";
        final ProcessingJob processingJob = processingJobDao.read(jobID);

        assertNotNull("Update the jobID to a valid ID", processingJob);

        final ProcessingJobStats processingJobStats = harvesterClient.statsOfJob(jobID);

        final Map<ProcessingState, Set<String>> recordIdsByState = processingJobStats.getRecordIdsByState();
        final Map<ProcessingState, Set<String>> sourceDocumentReferenceIdsByState =
                processingJobStats.getSourceDocumentReferenceIdsByState();

        for(final ProcessingState processingState : recordIdsByState.keySet()) {
            LOG.debug("State: {}", processingState);
            for(final String recordId : recordIdsByState.get(processingState)) {
                LOG.debug("\trecord id: {}", recordId);
            }
        }

        for(final ProcessingState processingState : sourceDocumentReferenceIdsByState.keySet()) {
            LOG.debug("State: {}", processingState);
            for(final String sourceDocId : sourceDocumentReferenceIdsByState.get(processingState)) {
                LOG.debug("\tsource doc id: {}", sourceDocId);
            }
        }
    }
}
