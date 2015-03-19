package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetDownloadSpeed implements Serializable {

    private final String taskID;

    public GetDownloadSpeed(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
