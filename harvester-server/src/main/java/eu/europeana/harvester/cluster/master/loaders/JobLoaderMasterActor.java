package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.Clean;
import eu.europeana.harvester.cluster.domain.messages.LoadJobs;
import eu.europeana.harvester.db.interfaces.MachineResourceReferenceDao;
import eu.europeana.harvester.db.interfaces.ProcessingJobDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.interfaces.SourceDocumentReferenceDao;
import eu.europeana.harvester.logging.LoggingComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JobLoaderMasterActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    /**
     * The cluster master is splitted into two separate actors.
     * This reference is reference to an actor which only receives messages from slave.
     */
    private final ActorRef receiverActor;

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
    private final DefaultLimits defaultLimits;

    /**
     * Maps each IP with a boolean which indicates if an IP has jobs in MongoDB or not.
     */
    private final HashMap<String, Boolean> ipsWithJobs;

    /**
     * An object which contains a list of IPs which has to be treated different.
     */
    private final IPExceptions ipExceptions;

    private ActorRef loaderActor = null;

    private long markLoad = 0;

    private boolean haveLoader = false;

    private final ActorRef limiterActor;

    public JobLoaderMasterActor(final ActorRef receiverActor, final ClusterMasterConfig clusterMasterConfig,
                                final ActorRef accountantActor, final ActorRef limiterActor, final ProcessingJobDao processingJobDao,
                                final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                final SourceDocumentReferenceDao SourceDocumentReferenceDao,
                                final MachineResourceReferenceDao machineResourceReferenceDao,
                                final DefaultLimits defaultLimits,
                                final HashMap<String, Boolean> ipsWithJobs, final IPExceptions ipExceptions) {
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                "The loader master is constructed");

        this.receiverActor = receiverActor;
        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.limiterActor = limiterActor;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.SourceDocumentReferenceDao = SourceDocumentReferenceDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.defaultLimits = defaultLimits;
        this.ipsWithJobs = ipsWithJobs;
        this.ipExceptions = ipExceptions;
        this.haveLoader = false;

        LOG.debug("Call check for abandoned jobs from constructor - job loader");

        JobLoaderMasterHelper.checkForAbandonedJobs(processingJobDao, clusterMasterConfig, LOG);

        LOG.debug("Call ip distribution from constructor - job loader");

        ipDistribution = JobLoaderMasterHelper.getIPDistribution(machineResourceReferenceDao, LOG);
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof LoadJobs) {


            if ( !haveLoader ) {

                try {

                    ActorRef loaderActor = JobLoaderExecutorActor.createActor(getContext().system(),
                            clusterMasterConfig, accountantActor,limiterActor, processingJobDao, sourceDocumentProcessingStatisticsDao,

                                                                              SourceDocumentReferenceDao, machineResourceReferenceDao, ipsWithJobs, ipExceptions, ipDistribution
                    );
                    context().watch(loaderActor);
                    loaderActor.tell(message, ActorRef.noSender());
                    haveLoader = true;
                    LOG.debug("Created loader actor {}", loaderActor);

                } catch (Exception e) {
                    LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Master.TASKS_LOADER),
                            "Exception while loading jobs", e);
                }

            }
            return;
        }
        if (message instanceof Clean) {
            LOG.debug("Message instance of clean");

            JobLoaderMasterHelper.checkForAbandonedJobs(processingJobDao, clusterMasterConfig, LOG);

            LOG.debug("Call ip distribution from message instanceof clean");

            this.ipDistribution = JobLoaderMasterHelper.getIPDistribution(machineResourceReferenceDao, LOG);

            getSelf().tell(new LoadJobs(), getSelf());

            LOG.debug("Called message clean");

            return;
        }

        if (message instanceof Terminated) {
            if (((Terminated) message).getActor()==loaderActor) {
                LOG.debug("Got terminated for {}, marking the loader as expired ", ((Terminated) message).getActor());
                haveLoader = false;
            }
            haveLoader = false;

        }
    }


}
