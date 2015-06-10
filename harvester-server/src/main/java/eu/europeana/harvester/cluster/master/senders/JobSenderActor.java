package eu.europeana.harvester.cluster.master.senders;

import akka.actor.ActorRef;
import akka.actor.UnhandledMessage;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.DefaultLimits;
import eu.europeana.harvester.cluster.domain.IPExceptions;
import eu.europeana.harvester.cluster.domain.messages.Clean;
import eu.europeana.harvester.cluster.domain.messages.RequestTasks;
import eu.europeana.harvester.cluster.master.MasterMetrics;
import scala.concurrent.duration.Duration;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class JobSenderActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * The cluster master is split into three separate actors.
     * This reference is reference to an actor which only receives messages from slave.
     */
    private ActorRef receiverActor;

    /**
     * The cluster master is split into three separate actors.
     * This reference is reference to an actor which only loads jobs from MongoDB.
     */
    private ActorRef jobLoaderActor;

    /**
     * A wrapper class for all important data (ips, loaded jobs, jobs in progress etc.)
     */
    private ActorRef accountantActor;



    /**
     * An object which contains a list of IPs which has to be treated different.
     */
    private final IPExceptions ipExceptions;




    /**
     * Contains default download limits.
     */
    private final DefaultLimits defaultLimits;


    /**
     * A list of tasks generated for a slave.
     */


    /**
     * The interval in hours when the master cleans itself and its slaves.
     */
    private final Integer cleanupInterval;

    /**
     * Maps each IP with a boolean which indicates if an IP has jobs in MongoDB or not.
     */
    private final HashMap<String, Boolean> ipsWithJobs = new HashMap<>();



    public JobSenderActor(final IPExceptions ipExceptions, final DefaultLimits defaultLimits,
                          final Integer cleanupInterval, final ActorRef jobLoaderActor,
                          final  ActorRef accountantActor, final ActorRef receiverActor
    ) {
        LOG.info("JobSenderActor constructor");

        this.ipExceptions = ipExceptions;
        this.defaultLimits = defaultLimits;
        this.cleanupInterval = cleanupInterval;

        this.accountantActor = accountantActor;
        this.jobLoaderActor = jobLoaderActor;
        this.receiverActor = receiverActor;



    }


    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof RequestTasks) {

            MasterMetrics.Master.sendJobSetToSlaveCounter.inc();
            final com.codahale.metrics.Timer.Context context = MasterMetrics.Master.sendJobSetToSlaveDuration.time();
            try {
                JobSenderHelper.handleRequest(getSender(), accountantActor, receiverActor, jobLoaderActor,
                        defaultLimits, ipsWithJobs, LOG, ipExceptions);
            } finally {
                context.stop();
            }
            return;
        }



        if(message instanceof Clean) {
            LOG.info("Cleaning up ClusterMasterActor and its slaves.");

            getContext().system().scheduler().scheduleOnce(Duration.create(cleanupInterval,
                    TimeUnit.HOURS), getSelf(), new Clean(), getContext().system().dispatcher(), getSelf());
            return;
        }

        if(message instanceof UnhandledMessage) {
            LOG.error((String) message);
            throw new NotImplementedException();
        }

    }






}