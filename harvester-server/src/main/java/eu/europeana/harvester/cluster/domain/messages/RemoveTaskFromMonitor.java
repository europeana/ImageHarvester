package eu.europeana.harvester.cluster.domain.messages;

import akka.actor.Address;

import java.io.Serializable;

public class RemoveTaskFromMonitor implements Serializable {

    final Address address;
    final String taskId;

    public RemoveTaskFromMonitor(final Address address, final String taskId) {
        this.address = address;
        this.taskId = taskId;
    }

    public Address getAddress() { return address; }

    public String getTaskId() { return taskId; }
}
