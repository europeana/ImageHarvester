package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class AddDownloadSpeed implements Serializable {

    private final String taskID;

    private final Long speed;

    public AddDownloadSpeed(String taskID, Long speed) {
        this.taskID = taskID;
        this.speed = speed;
    }

    public String getTaskID() {
        return taskID;
    }

    public Long getSpeed() {
        return speed;
    }
}
