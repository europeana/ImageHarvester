package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.cluster.domain.TaskState;
import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;

import java.io.Serializable;

public class ModifyState implements Serializable {

    private final String taskID;

    private final String jobId;

    private final String IP;

    private final TaskState state;

    private final DoneProcessing doneProcessing;

    public ModifyState(String taskID, String jobId, String IP,DoneProcessing doneProcessing, TaskState state) {
        this.taskID = taskID;
        this.jobId = jobId;
        this.IP = IP;
        this.doneProcessing = doneProcessing;
        this.state = state;
    }

    public String getTaskID() {
        return taskID;
    }
    public String getJobId() { return jobId;}
    public String getIP() { return IP; }

    public DoneProcessing getDoneProcessing() {
        return doneProcessing;
    }

    public TaskState getState() {
        return state;
    }
}
