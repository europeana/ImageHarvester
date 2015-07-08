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

            final Boolean auth = datastore.getMongo().getDB("admin").authenticate(mongoConfig.getUsername(),
                    mongoConfig.getPassword()
                            .toCharArray());

            if (!auth) {
                throw new MongoException("Cannot authenticate to mongo database");
            }
        }

        harvesterClient = new HarvesterClientImpl(datastore.getDatastore(), new HarvesterClientConfig(WriteConcern.SAFE));

    }

    public void saveSourceDocumentReferences(final List<SourceDocumentReference> sourceDocumentReferences,final String migratingBatchId) throws MalformedURLException, UnknownHostException, InterruptedException, ExecutionException, TimeoutException {
        try{
            harvesterClient.createOrModifySourceDocumentReference(sourceDocumentReferences);
        }
        finally {
            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_HARVESTER,migratingBatchId,null,null),
                    "Finished saving {} source document references", sourceDocumentReferences.size());
        }
    }

    public void saveProcessingJobs(final List<ProcessingJob> jobs,final String migratingBatchId) {
        try {
            for (final ProcessingJob processingJob : jobs) {
                harvesterClient.createOrModify(processingJob);
            }
        }
        finally {
            LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PERSISTENCE_HARVESTER,migratingBatchId,null,null),
                    "Finished saving {} jobs", jobs.size());
        }
    }
}

