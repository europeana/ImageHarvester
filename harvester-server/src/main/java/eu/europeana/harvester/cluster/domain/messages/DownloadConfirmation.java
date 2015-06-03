package eu.europeana.harvester.cluster.domain.messages;

import eu.europeana.harvester.httpclient.response.RetrievingState;

import java.io.Serializable;

public class DownloadConfirmation implements Serializable {

    private final String taskID;

    private final String ipAddress;

    private final RetrievingState state;

    public DownloadConfirmation(String taskID, String ipAddress, final RetrievingState state) {
        this.taskID = taskID;
        this.ipAddress = ipAddress;
        this.state = state;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public String getTaskID() {
        return taskID;
    }

    public RetrievingState getState() {
        return state;
    }
}
