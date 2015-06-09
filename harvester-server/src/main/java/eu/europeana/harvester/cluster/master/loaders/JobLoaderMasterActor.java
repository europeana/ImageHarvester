package eu.europeana.harvester.cluster.master.loaders;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.ClusterMasterConfig;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.Clean;
import eu.europeana.harvester.cluster.domain.messages.LoadJobs;
import eu.europeana.harvester.cluster.domain.messages.LookInDB;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.db.ProcessingJobDao;
import eu.europeana.harvester.db.SourceDocumentProcessingStatisticsDao;
import eu.europeana.harvester.db.SourceDocumentReferenceDao;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JobLoaderMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

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
     * A map with all system addresses which maps each address with a list of actor refs.
     * This is needed if we want to clean them or if we want to broadcast a message.
     */
    private final Map<Address, HashSet<ActorRef>> actorsPerAddress;

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
    private final SourceDocumentReferenceDao sourceDocumentReferenceDao;

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


    public JobLoaderMasterActor(final ActorRef receiverActor, final ClusterMasterConfig clusterMasterConfig,
                                final ActorRef accountantActor, final Map<Address, HashSet<ActorRef>> actorsPerAddress,
                                final ProcessingJobDao processingJobDao,
                                final SourceDocumentProcessingStatisticsDao sourceDocumentProcessingStatisticsDao,
                                final SourceDocumentReferenceDao sourceDocumentReferenceDao,
                                final MachineResourceReferenceDao machineResourceReferenceDao,
                                final DefaultLimits defaultLimits,
                                final HashMap<String, Boolean> ipsWithJobs, final IPExceptions ipExceptions) {
        LOG.info("JobLoaderMasterActor constructor");

        this.receiverActor = receiverActor;
        this.clusterMasterConfig = clusterMasterConfig;
        this.accountantActor = accountantActor;
        this.actorsPerAddress = actorsPerAddress;
        this.processingJobDao = processingJobDao;
        this.sourceDocumentProcessingStatisticsDao = sourceDocumentProcessingStatisticsDao;
        this.sourceDocumentReferenceDao = sourceDocumentReferenceDao;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.defaultLimits = defaultLimits;
        this.ipsWithJobs = ipsWithJobs;
        this.ipExceptions = ipExceptions;

        JobLoaderMasterHelper.checkForAbandonedJobs(processingJobDao, clusterMasterConfig, LOG);
        JobLoaderMasterHelper.getIPDistribution(machineResourceReferenceDao, LOG);
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof LoadJobs) {


            if (loaderActor == null ) {

                try {

                    ActorRef loaderActor = JobLoaderExecutorActor.createActor(getContext().system(),
                            clusterMasterConfig, accountantActor, processingJobDao, sourceDocumentProcessingStatisticsDao,
                            sourceDocumentReferenceDao, machineResourceReferenceDao, ipsWithJobs, ipExceptions, ipDistribution
                    );
                    context().watch(loaderActor);
                    loaderActor.tell(message, ActorRef.noSender());

                } catch (Exception e) {
                    LOG.error("Error in LoadJobs: " + e.getMessage());
                }

            } else
                LOG.info("LoaderActor still working");

            return;
        }
        if (message instanceof LookInDB) {
            JobLoaderMasterHelper.updateLists(clusterMasterConfig, processingJobDao, sourceDocumentProcessingStatisticsDao,
                    sourceDocumentReferenceDao, accountantActor, LOG);

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                    TimeUnit.MINUTES), getSelf(), new LookInDB(), getContext().system().dispatcher(), getSelf());
        }
        if (message instanceof Clean) {
            LOG.info("Clean JobLoaderMasterActor");
            JobLoaderMasterHelper.checkForAbandonedJobs(processingJobDao, clusterMasterConfig, LOG);
            this.ipDistribution = JobLoaderMasterHelper.getIPDistribution(machineResourceReferenceDao, LOG);

            getSelf().tell(new LoadJobs(), getSelf());
            return;
        }

        if (message instanceof Terminated) {
            loaderActor = null;
        }
    }


}
