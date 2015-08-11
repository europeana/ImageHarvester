package eu.europeana.harvester.cluster.master.limiter.domain;

public class ReserveConnectionSlotRequest {

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
