package eu.europeana.harvester.cluster.master.limiter;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.master.limiter.domain.*;
import eu.europeana.harvester.cluster.master.metrics.MasterMetrics;
import eu.europeana.harvester.logging.LoggingComponent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class IPLimiterAccountantActor extends UntypedActor {

    public static final ActorRef createActor(final ActorSystem system,
                                             final IPLimiterConfig IPLimiterConfig, final String name
    ) {
        return system.actorOf(Props.create(IPLimiterAccountantActor.class,
                IPLimiterConfig
        ), name);
    }

    public static final ActorRef createActor(final ActorSystem system,
                                             final IPLimiterConfig IPLimiterConfig
    ) {
        return createActor(system, IPLimiterConfig, "masterLimiter");
    }

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private final IpLimiterAccountant ipLimiterAccountant;
    private final IPLimiterConfig IPLimiterConfig;

    public IPLimiterAccountantActor(final IPLimiterConfig IPLimiterConfig) {
        this.ipLimiterAccountant = new IpLimiterAccountant(IPLimiterConfig.getDefaultLimitsPerIp(), IPLimiterConfig.getSpecificLimitsPerIp());
        this.IPLimiterConfig = IPLimiterConfig;
    }

    @Override
    public void preStart() throws Exception {
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.IP_LIMITER),
                "IP limiter pre starting.");

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(IPLimiterConfig.getMaxSlotUsageLife().getStandardSeconds(),
                TimeUnit.SECONDS), getSelf(), new IPLimitCleanExpiredSlots(), getContext().system().dispatcher(), getSelf());
    }


    @Override
    public void onReceive(Object message) throws Exception {

        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.IP_LIMITER), "IP limiter on receive");

        if (message instanceof ReserveConnectionSlotRequest) {
            LOG.debug("IO limiter instanceof ReserveConnectionSlotRequest, message task id:" + ((ReserveConnectionSlotRequest) message).getTaskID());

            final ReserveConnectionSlotRequest reserveConnectionSlotRequest = (ReserveConnectionSlotRequest) message;
            final ReserveConnectionSlotResponse response = ipLimiterAccountant.reserveConnectionSlotRequest(reserveConnectionSlotRequest);
            if (response.getGranted()) MasterMetrics.Master.ipLimitGrantedSlotRequestCounter.inc();
            else MasterMetrics.Master.ipLimitNotGrantedSlotRequestCounter.inc();

            getSender().tell(response, getSelf());
            return;
        }
        if (message instanceof ReturnConnectionSlotRequest) {
            LOG.debug("IO limiter instanceof ReturnConnectionSlotRequest, message slot id:" + ((ReturnConnectionSlotRequest) message).getSlotId());

            final ReturnConnectionSlotRequest returnConnectionSlotRequest = (ReturnConnectionSlotRequest) message;
            ipLimiterAccountant.returnConnectionSlotRequest(returnConnectionSlotRequest);
            MasterMetrics.Master.ipLimitReturnedGrantedSlotRequestCounter.inc();
            return;
        }
        if (message instanceof IPLimitCleanExpiredSlots) {
            LOG.debug("IO limiter instanceof IPLimitCleanExpiredSlots");

            cleanExpiredSlots();
            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(IPLimiterConfig.getMaxSlotUsageLife().getStandardSeconds(),
                    TimeUnit.SECONDS), getSelf(), new IPLimitCleanExpiredSlots(), getContext().system().dispatcher(), getSelf());
            return ;
        }

        if (message instanceof  ChangeMaxAvailableSlotsRequest) {
            LOG.debug("IO limiter instanceof ChangeMaxAvailableSlotsRequest");

            final ChangeMaxAvailableSlotsRequest changeMaxAvailableSlotsRequest = (ChangeMaxAvailableSlotsRequest) message;
            ipLimiterAccountant.setSpecificLimitPerIp(changeMaxAvailableSlotsRequest.getIp(),changeMaxAvailableSlotsRequest.getMaxAvailableSlots());
            return ;
        }

        unhandled(message);

    }

    private final void cleanExpiredSlots() {
        final int reclaimedSlots = ipLimiterAccountant.reclaimOccupiedSlotsOlderThan(DateTime.now().minus(IPLimiterConfig.getMaxSlotUsageLife()));
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Master.IP_LIMITER),
                "IP limiter reclaimed {} slots. Next reclaiming will execute in {} seconds.", reclaimedSlots, IPLimiterConfig.getMaxSlotUsageLife().toStandardSeconds().getSeconds());

    }
}

