package eu.europeana.harvester.cluster.master;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.UntypedActor;
import eu.europeana.harvester.cluster.domain.messages.AddAddressToMonitor;
import eu.europeana.harvester.cluster.domain.messages.AddTaskToMonitor;
import eu.europeana.harvester.cluster.domain.messages.Monitor;
import eu.europeana.harvester.cluster.domain.messages.RemoveTaskFromMonitor;
import eu.europeana.harvester.logging.LoggingComponent;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ClusterMasterMonitoringActor extends UntypedActor {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());


    /**
     * A map with all system addresses which maps each address with a list of actor refs.
     * This is needed if we want to clean them or if we want to broadcast a message.
     */
    private final Map<Address, HashSet<ActorRef>> actorsPerAddress;

    /**
     * A map with all system addresses which maps each address with a set of tasks.
     * This is needed to restore the tasks if a system crashes.
     */
    private final Map<Address, HashSet<String>> tasksPerAddress;

    /**
     * A map with all sent but not confirmed tasks which maps these tasks with a datetime object.
     * It's needed to restore all the tasks which are not confirmed after a given period of time.
     */
    private final Map<String, DateTime> tasksPerTime;




    public ClusterMasterMonitoringActor() {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Master.CLUSTER_MASTER),
                "ClusterMasterMonitoringActor constructor");


        this.actorsPerAddress = new HashMap<>();
        this.tasksPerAddress = new HashMap<>();
        this.tasksPerTime = new HashMap<>();
    }


    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof Monitor) {
            monitor();
            return;
        }

        if(message instanceof AddAddressToMonitor) {
            AddAddressToMonitor address = (AddAddressToMonitor) message;
            addAddress(address.getAddress(), address.getActorRef());
            return;
        }

        if(message instanceof AddTaskToMonitor) {
            AddTaskToMonitor task = (AddTaskToMonitor) message;
            addTask(task.getAddress(), task.getTaskId());
            return;
        }

        if(message instanceof RemoveTaskFromMonitor) {
            RemoveTaskFromMonitor task = (RemoveTaskFromMonitor) message;
            removeTask(task.getAddress(), task.getTaskId());
            return;
        }


    }



    // TODO : Refactor this as it polutes the logstash index.
    private void monitor() {
        LOG.info("Active nodes: {}", tasksPerAddress.size());
        LOG.info("Actors per node: ");
        for(final Map.Entry<Address, HashSet<ActorRef>> elem : actorsPerAddress.entrySet()) {
            LOG.info("Address: {}", elem.getKey());
            for(final ActorRef actor : elem.getValue()) {
                LOG.info("\t{}", actor);
            }
        }

        LOG.info("Tasks: ");
        for(final Map.Entry<Address, HashSet<String>> elem : tasksPerAddress.entrySet()) {
            LOG.info("Address: {}, nr of requests: {}", elem.getKey(), elem.getValue().size());
        }
    }



    /**
     * Stores an address and an actorRef from that address.
     * @param address actor systems address
     * @param actorRef reference to an actor from the actor system
     */
    private void addAddress(final Address address, final ActorRef actorRef) {
        if(actorsPerAddress.containsKey(address)) {
            final HashSet<ActorRef> actorRefs = actorsPerAddress.get(address);
            actorRefs.add(actorRef);

            actorsPerAddress.put(address, actorRefs);
        } else {
            final HashSet<ActorRef> actorRefs = new HashSet<>();
            actorRefs.add(actorRef);

            actorsPerAddress.put(address, actorRefs);
        }
    }

    /**
     * Stores a reference to a tasks which can be reached by an actor system address
     * @param address actor systems address
     * @param taskId ID of the started task
     */
    private void addTask(final Address address, final String taskId) {
        tasksPerTime.remove(taskId);

        if(tasksPerAddress.containsKey(address)) {
            final HashSet<String> tasks = tasksPerAddress.get(address);
            tasks.add(taskId);

            tasksPerAddress.put(address, tasks);
        } else {
            final HashSet<String> tasks = new HashSet<>();
            tasks.add(taskId);

            tasksPerAddress.put(address, tasks);
        }
    }

    /**
     * Removes the reference of a task after it was finished by a slave
     * @param address actor systems address
     * @param taskId ID of the completed task
     */
    private void removeTask(final Address address, final String taskId) {
        if(tasksPerAddress.containsKey(address)) {
            final HashSet<String> tasks = tasksPerAddress.get(address);
            tasks.remove(taskId);

            tasksPerAddress.put(address, tasks);
        }
    }




}
