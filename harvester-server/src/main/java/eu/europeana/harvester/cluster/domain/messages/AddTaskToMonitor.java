package eu.europeana.harvester.cluster.domain.messages;

import akka.actor.Address;

import java.io.Serializable;

public class AddTaskToMonitor implements Serializable {

    final Address address;
    final String taskId;

    public AddTaskToMonitor(final Address address, final String taskId) {
        this.address = address;
        this.taskId = taskId;
    }

    public Address getAddress() { return address; }

    public String getTaskId() { return taskId; }
}
