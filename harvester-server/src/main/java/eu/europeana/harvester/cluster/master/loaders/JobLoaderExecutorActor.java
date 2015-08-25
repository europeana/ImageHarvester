package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.*;
import com.codahale.metrics.Timer;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.LoadJobs;
import eu.europeana.harvester.cluster.master.metrics.MasterMetrics;
import eu.europeana.harvester.db.interfaces.MachineResourceReferenceDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JobLoaderExecutorActor extends UntypedActor {

    public static final ActorRef createActor(final ActorSystem system, final ClusterMasterConfig clusterMasterConfig,
                                             final ActorRef accountantActor,final ActorRef limiterActor, final ProcessingJobDao processingJobDao,
                                             final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                             final SourceDocumentReferenceDao SourceDocumentReferenceDao,
                                             final MachineResourceReferenceDao machineResourceReferenceDao,
                                             final HashMap<String, Boolean> ipsWithJobs, final IPExceptions ipExceptions,
                                             final Map<String, Integer> ipDistribution
    ) {
        return system.actorOf(Props.create(JobLoaderExecutorActor.class,
                clusterMasterConfig, accountantActor,limiterActor, processingJobDao, sourceDocumentProcessingStatisticsDao,
                                           SourceDocumentReferenceDao, machineResourceReferenceDao, ipsWithJobs, ipExceptions, ipDistribution ));

    }

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());


    /**
     * Contains all the configuration needed by this actor.
     */
    private final ClusterMasterConfig clusterMasterConfig;

    /**
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;

    /**
     * ProcessingJob DAO object which lets us to read and store data to and from the database.
     */
    private final ProcessingJobDao processingJobDao;

    /**
     * SourceDocumentProcessingStatistics DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao;

    /**
     * SourceDocumentReference DAO object which lets us to read and store data to and from the database.
     */
    private final SourceDocumentReferenceDao SourceDocumentReferenceDao;

    /**
     * MachineResourceReference DAO object which lets us to read and store data to and from the database.
     */
    private final MachineResourceReferenceDao machineResourceReferenceDao;

    /**
     * A map which maps each ip with the number of jobs from that ip.
     */
    private Map<String, Integer> ipDistribution;

    /**
     * Contains default download limits.
     */

    /**
     * Maps each IP with a boolean which indicates if an IP has jobs in MongoDB or not.
     */
    private final HashMap<String, Boolean> ipsWithJobs;

    /**
     * An object which contains a list of IPs which has to be treated different.
     */
    private final IPExceptions ipExceptions;

    private final ActorRef limiterActor;

    public JobLoaderExecutorActor(final ClusterMasterConfig clusterMasterConfig,
                                  final ActorRef accountantActor,final ActorRef limiterActor, final ProcessingJobDao processingJobDao,
                                  final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                  final SourceDocumentReferenceDao SourceDocumentReferenceDao,
                                  final MachineResourceReferenceDao machineResourceReferenceDao,
                                  final HashMap<String, Boolean> ipsWithJobs, final IPExceptions ipExceptions,
                                  final Map<String, Integer> ipDistribution) {
//        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
//                "The loader executor is constructed");

        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.limiterActor = limiterActor;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.SourceDocumentReferenceDao = SourceDocumentReferenceDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.ipsWithJobs = ipsWithJobs;
        this.ipExceptions = ipExceptions;
        this.ipDistribution = ipDistribution;


    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof LoadJobs) {


            final Timer.Context context = MasterMetrics.Master.loadJobFromDBDuration.time();
            try {

                JobLoaderExecutorHelper.checkForNewFastLaneJobs(clusterMasterConfig, ipDistribution, ipsWithJobs, accountantActor,limiterActor, processingJobDao,
                                                                SourceDocumentReferenceDao, machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao, LOG);

                JobLoaderExecutorHelper.checkForNewJobs(clusterMasterConfig, ipDistribution, ipsWithJobs,
                                                        accountantActor,limiterActor, processingJobDao,
                                                        SourceDocumentReferenceDao, machineResourceReferenceDao, sourceDocumentProcessingStatisticsDao, LOG);

            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                        "Exception while loading jobs", e);
            }
            context.stop();
            self().tell(PoisonPill.getInstance(), ActorRef.noSender());

            return;
        }
    }

}