package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;
import java.util.List;

public class BagOfTasks implements Serializable {

    private final List<RetrieveUrl> tasks;

    public BagOfTasks(List<RetrieveUrl> tasks) {
        this.tasks = tasks;
    }

    public List<RetrieveUrl> getTasks() {
        return tasks;
    }
}
