package eu.europeana.crfmigration.dao;

import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import eu.europeana.harvester.domain.MongoConfig;
import eu.europeana.crfmigration.logging.LoggingComponent;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.MorphiaDataStore;
import eu.europeana.harvester.domain.ProcessingJob;
import eu.europeana.harvester.domain.SourceDocumentReference;
import eu.europeana.jobcreator.domain.ProcessingJobTuple;
import org.apache.maven.shared.utils.StringUtils;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class MigratorHarvesterDao {
    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final HarvesterClientImpl harvesterClient;

    public MigratorHarvesterDao (MongoConfig mongoConfig) throws UnknownHostException {

        final MorphiaDataStore datastore =
                new MorphiaDataStore(mongoConfig.getMongoServerAddressList(), mongoConfig.getDbName());


        if (StringUtils.isNotEmpty(mongoConfig.getUsername())  &&
                StringUtils.isNotEmpty(mongoConfig.getPassword())) {

            final Boolean auth = datastore.getMongo().getDB("admin").authenticate(
                    mongoConfig.getUsername(),
                    mongoConfig.getPassword().toCharArray());

            if (!auth) {
                throw new MongoException("Cannot authenticate to mongo database");
            }
        }

        harvesterClient = new HarvesterClientImpl(datastore.getDatastore(), new HarvesterClientConfig(WriteConcern.UNACKNOWLEDGED));

    }

    public void saveProcessingJobTuples (List<ProcessingJobTuple> processingJobTuples, String migratingBatchId) throws
                                                                                                                InterruptedException,
                                                                                                                MalformedURLException,
                                                                                                                TimeoutException,
                                                                                                                ExecutionException,
                                                                                                                UnknownHostException {
            harvesterClient.createOrModifyProcessingJobTuples(processingJobTuples);
    }
}

