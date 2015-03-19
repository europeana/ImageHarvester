package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

public class DownloadConfirmation implements Serializable{

    private final String taskID;

    private final String ipAddress;

    public DownloadConfirmation(String taskID, String ipAddress) {
        this.taskID = taskID;
        this.ipAddress = ipAddress;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getTaskID() {
        return taskID;
    }
}
