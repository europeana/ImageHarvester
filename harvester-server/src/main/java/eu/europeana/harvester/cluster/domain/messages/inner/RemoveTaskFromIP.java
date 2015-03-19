package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class RemoveTaskFromIP implements Serializable {

    private final String taskID;

    private final String IP;


    public RemoveTaskFromIP(String taskID, String ip) {
        this.taskID = taskID;
        IP = ip;
    }

    public String getTaskID() {
        return taskID;
    }

    public String getIP() {
        return IP;
    }
}
