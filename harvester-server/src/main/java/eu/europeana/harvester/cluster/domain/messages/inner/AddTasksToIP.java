package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;
import java.util.List;

public class AddTasksToIP implements Serializable {

    private final String IP;

    private final List<String> tasks;

    public AddTasksToIP(String ip, List<String> tasks) {
        IP = ip;
        this.tasks = tasks;
    }

    public String getIP() {
        return IP;
    }

    public List<String> getTasks() {
        return tasks;
    }
}
