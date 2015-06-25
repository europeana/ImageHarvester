package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class AddTasksToIP implements Serializable {

    private final String IP;

    private final String task;

    public AddTasksToIP(String ip, String task) {
        IP = ip;
        this.task = task;
    }

    public String getIP() {
        return IP;
    }

    public String getTask() {
        return task;
    }
}
