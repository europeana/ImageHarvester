package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import eu.europeana.harvester.cluster.domain.PingMasterConfig;
import eu.europeana.harvester.cluster.domain.messages.DonePing;
import eu.europeana.harvester.cluster.domain.messages.LookInDB;
import eu.europeana.harvester.cluster.domain.messages.StartPing;
import eu.europeana.harvester.db.MachineResourceReferenceDao;
import eu.europeana.harvester.db.MachineResourceReferenceStatDao;
import eu.europeana.harvester.domain.MachineResourceReference;
import eu.europeana.harvester.domain.MachineResourceReferenceStat;
import eu.europeana.harvester.domain.Page;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This actor coordinates the tasks related to pings.
 */
public class PingMasterActor extends UntypedActor {

    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    /**
     * The number of active nodes. If there is no active node the ping master waits with the sending of ping tasks.
     */
    private int activeNodes = 0;

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
        LOG.debug("Started ping master actor");

        final Cluster cluster = Cluster.get(getContext().system());
        cluster.subscribe(getSelf(), ClusterEvent.initialStateAsEvents(), MemberEvent.class, UnreachableMember.class);

        getContext().setReceiveTimeout(scala.concurrent.duration.Duration.create(
                pingMasterConfig.getReceiveTimeoutInterval().getStandardSeconds(), TimeUnit.SECONDS));
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof LookInDB) {
            LOG.info("=========== Starts pinging ===========");
            start();
            LOG.info("======================================");

            return;
        }
        if(message instanceof DonePing) {
            save((DonePing) message);

            return;
        }

        // cluster events
        if (message instanceof MemberUp) {
            final ClusterEvent.MemberUp mUp = (MemberUp) message;
            LOG.info("Member is Up: {}", mUp.member());

            final boolean self =
                    mUp.member().address().equals(Cluster.get(getContext().system()).selfAddress());

            if(!self) {
                activeNodes++;
            }

            return;
        }
        if (message instanceof ClusterEvent.UnreachableMember) {
            final ClusterEvent.UnreachableMember mUnreachable = (ClusterEvent.UnreachableMember) message;
            LOG.info("Member detected as Unreachable: {}", mUnreachable.member());

            return;
        }
        if (message instanceof ClusterEvent.MemberRemoved) {
            final ClusterEvent.MemberRemoved mRemoved = (ClusterEvent.MemberRemoved) message;
            LOG.info("Member is Removed: {}", mRemoved.member());

            activeNodes--;

            return;
        }
    }

    /**
     * This is the heart of the ping master. This checks periodically for new jobs and executes them.
     */
    private void start() {
        if(activeNodes == 0) {
            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(10,
                    TimeUnit.MINUTES), getSelf(), new LookInDB(), getContext().system().dispatcher(), getSelf());
        } else {
            updateList();

            getContext().system().scheduler().scheduleOnce(scala.concurrent.duration.Duration.create(pingMasterConfig.getNewPingInterval(),
                    TimeUnit.MILLISECONDS), getSelf(), new LookInDB(), getContext().system().dispatcher(), getSelf());
        }
    }

    /**
     * Looks in the database for new jobs and executes them.
     */
    private void updateList() {
        boolean done = false;
        int last = 0;

        final List<MachineResourceReference> machineResourceReferenceList = new ArrayList<MachineResourceReference>();
        while(!done) {
            final Page page = new Page(last, 10000);
            last += 10000;
            final List<MachineResourceReference> temp =
                    machineResourceReferenceDao.getAllMachineResourceReferences(page);

            machineResourceReferenceList.addAll(temp);
            if(temp == null || temp.size() == 0) {
                done = true;
            }
        }

        for(MachineResourceReference machineResourceReference : machineResourceReferenceList) {
            ipAndLastCheck.put(machineResourceReference.getId(), System.currentTimeMillis());

            machineResourceReference = machineResourceReference.withLastPingCheck(new Date());
            machineResourceReferenceDao.update(machineResourceReference, pingMasterConfig.getWriteConcern());

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
        LOG.debug("Check ip: " + ip);

        final StartPing startPing = new StartPing(ip, nrOfPings, pingTimeout);
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

            machineResourceReferenceStatDao.create(machineResourceReferenceStat, pingMasterConfig.getWriteConcern());
        } else {
            final MachineResourceReferenceStat machineResourceReferenceStat =
                    new MachineResourceReferenceStat(null, new Date(), null, null, null, null);
            machineResourceReferenceStatDao.create(machineResourceReferenceStat, pingMasterConfig.getWriteConcern());
        }
    }
}
