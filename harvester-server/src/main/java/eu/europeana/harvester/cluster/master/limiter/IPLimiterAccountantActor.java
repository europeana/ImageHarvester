package eu.europeana.harvester.cluster.master.limiter;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.master.limiter.domain.*;
import eu.europeana.harvester.cluster.master.metrics.MasterMetrics;
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
//        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.IP_LIMITER),
//                "IP limiter pre starting.");

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(IPLimiterConfig.getMaxSlotUsageLife().getStandardSeconds(),
                TimeUnit.SECONDS), getSelf(), new IPLimitCleanExpiredSlots(), getContext().system().dispatcher(), getSelf());
    }


    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof ReserveConnectionSlotRequest) {
            final ReserveConnectionSlotRequest reserveConnectionSlotRequest = (ReserveConnectionSlotRequest) message;
            final ReserveConnectionSlotResponse response = ipLimiterAccountant.reserveConnectionSlotRequest(reserveConnectionSlotRequest);
            if (response.getGranted()) MasterMetrics.Master.ipLimitGrantedSlotRequestCounter.inc();
            else MasterMetrics.Master.ipLimitNotGrantedSlotRequestCounter.inc();

            getSender().tell(response, getSelf());
            return;
        }
        if (message instanceof ReturnConnectionSlotRequest) {
            final ReturnConnectionSlotRequest returnConnectionSlotRequest = (ReturnConnectionSlotRequest) message;
            ipLimiterAccountant.returnConnectionSlotRequest(returnConnectionSlotRequest);
            MasterMetrics.Master.ipLimitReturnedGrantedSlotRequestCounter.inc();
            return;
        }
        if (message instanceof IPLimitCleanExpiredSlots) {
            cleanExpiredSlots();
            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(IPLimiterConfig.getMaxSlotUsageLife().getStandardSeconds(),
                    TimeUnit.SECONDS), getSelf(), new IPLimitCleanExpiredSlots(), getContext().system().dispatcher(), getSelf());
            return ;
        }

        if (message instanceof  ChangeMaxAvailableSlotsRequest) {
            final ChangeMaxAvailableSlotsRequest changeMaxAvailableSlotsRequest = (ChangeMaxAvailableSlotsRequest) message;
            ipLimiterAccountant.setSpecificLimitPerIp(changeMaxAvailableSlotsRequest.getIp(),changeMaxAvailableSlotsRequest.getMaxAvailableSlots());
            return ;
        }

        unhandled(message);

    }

    private final void cleanExpiredSlots() {
        final int reclaimedSlots = ipLimiterAccountant.reclaimOccupiedSlotsOlderThan(DateTime.now().minus(IPLimiterConfig.getMaxSlotUsageLife()));
//        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.IP_LIMITER),
//                "IP limiter reclaimed {} slots. Next reclaiming will execute in {} seconds.", reclaimedSlots, IPLimiterConfig.getMaxSlotUsageLife().toStandardSeconds().getSeconds());

    }
}

