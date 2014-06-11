package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.PingMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.ActorStart;
import eu.europeana.harvester.cluster.domain.messages.DonePing;
import eu.europeana.harvester.cluster.domain.messages.StartPing;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.db.MachineResourceReferenceStatDao;
import eu.europeana.harvester.domain.MachineResourceReference;
import eu.europeana.harvester.domain.MachineResourceReferenceStat;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This actor coordinates the tasks related to pings.
 */
public class PingMasterActor extends UntypedActor {

    private final LoggingAdapter log = Logging.getLogger(getContext().system(), this);

    /**
     * Contains all the configuration needed by this actor.
     */
    private final PingMasterConfig pingMasterConfig;

    /**
     * The routers reference. We send all the messages to the router actor and then he decides the next step.
     */
    private final ActorRef routerActor;

    /**
     * MachineResourceReference DAO object which lets us to read and store data to and from the database.
     */
    private final MachineResourceReferenceDao machineResourceReferenceDao;

    /**
     * MachineResourceReferenceStat DAO object which lets us to read and store data to and from the database.
     */
    private final MachineResourceReferenceStatDao machineResourceReferenceStatDao;

    /**
     * Map which maps an ip with a number which represents the last ip check.
     */
    private final HashMap<String, Long> ipAndLastCheck = new HashMap<String, Long>();

    public PingMasterActor(PingMasterConfig pingMasterConfig, ActorRef routerActor,
                           MachineResourceReferenceDao machineResourceReferenceDao,
                           MachineResourceReferenceStatDao machineResourceReferenceStatDao) {
        this.pingMasterConfig = pingMasterConfig;

        this.routerActor = routerActor;
        this.machineResourceReferenceDao = machineResourceReferenceDao;
        this.machineResourceReferenceStatDao = machineResourceReferenceStatDao;
    }

    @Override
    public void preStart() throws Exception {
        log.debug("Started");

        getContext().setReceiveTimeout(scala.concurrent.duration.Duration.create(
                pingMasterConfig.getReceiveTimeoutInterval().getStandardSeconds(), TimeUnit.SECONDS));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof ActorStart) {
            start();
        } else
        if(message instanceof DonePing) {
            save((DonePing)message);
        }
    }

    /**
     * This is the heart of the ping master. This checks periodically for new jobs and executes them.
     */
    private void start() {
        log.info("====================== Starts pinging =================================");
        updateList();
        final int period = (int)pingMasterConfig.getNewPingInterval().getStandardSeconds();

        getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(period,
                TimeUnit.SECONDS), getSelf(), new ActorStart(), getContext().system().dispatcher(), getSelf());
    }

    /**
     * Looks in the database for new jobs and executes them.
     */
    private void updateList() {
        final List<MachineResourceReference> machineResourceReferenceList =
                machineResourceReferenceDao.getAllMachineResourceReferences();

        for(MachineResourceReference machineResourceReference : machineResourceReferenceList) {
            ipAndLastCheck.put(machineResourceReference.getId(), System.currentTimeMillis());

            machineResourceReference = machineResourceReference.withLastPingCheck(new Date());
            machineResourceReferenceDao.update(machineResourceReference);

            checkIP(machineResourceReference.getId(), pingMasterConfig.getNrOfPings(),
                    pingMasterConfig.getPingTimeout());
        }
    }

    /**
     * Starts a task.
     * @param ip servers ip
     * @param nrOfPings number of packets sent to server
     * @param pingTimeout time in milliseconds after the ping stops if the server is not responding
     */
    private void checkIP(String ip, int nrOfPings, int pingTimeout) {
        log.info("Check ip: " + ip);

        final StartPing startPing =
                new StartPing(ip, nrOfPings, pingTimeout);
        routerActor.tell(startPing, getSelf());
    }

    /**
     * Saves to the database the result of the ping task
     * @param donePing - message got from the slaves
     */
    private void save(DonePing donePing) {
        if(donePing.getSuccess()) {
            final MachineResourceReferenceStat machineResourceReferenceStat =
                    new MachineResourceReferenceStat(donePing.getIpAddress(), new Date(), donePing.getAvgTime(),
                            donePing.getMinTime(), donePing.getMaxTime(), donePing.getMedianDeviation());

            machineResourceReferenceStatDao.create(machineResourceReferenceStat);
        } else {
            final MachineResourceReferenceStat machineResourceReferenceStat =
                    new MachineResourceReferenceStat(null, new Date(), null, null, null, null);
            machineResourceReferenceStatDao.create(machineResourceReferenceStat);
        }
    }
}
