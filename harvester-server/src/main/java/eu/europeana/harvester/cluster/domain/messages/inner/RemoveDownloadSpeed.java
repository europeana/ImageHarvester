package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class RemoveDownloadSpeed implements Serializable {

    private final String taskID;

    public RemoveDownloadSpeed(String taskID) {
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
