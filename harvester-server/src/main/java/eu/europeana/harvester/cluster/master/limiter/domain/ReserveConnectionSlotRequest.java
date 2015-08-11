package eu.europeana.harvester.cluster.master.limiter.domain;

import java.io.Serializable;

public class ReserveConnectionSlotRequest implements Serializable {

    private final String ip;

    private final String taskID;


    public ReserveConnectionSlotRequest(String ip, String taskID) {
        this.ip = ip;
        this.taskID = taskID;
    }


    public String getTaskID() {
        return taskID;
    }

    public String getIp() {
        return ip;
    }
}
