package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetRetrieveUrl implements Serializable{

    private final String IP;
    private final String taskID;
    private final boolean isException;
    private final Long defaultLimit;
    private final int exceptionLimit;

    public GetRetrieveUrl ( String taskID, String IP, boolean isException, Long defaultLimit, int exceptionLimit) {
        this.IP = IP;
        this.isException = isException;
        this.defaultLimit = defaultLimit;
        this.exceptionLimit = exceptionLimit;
        this.taskID = taskID;
    }

    public String getIP() {
        return IP;
    }

    public String getTaskID(){
        return taskID;
    }

    public boolean isIPException() {
        return isException;
    }

    public Long getDefaultLimit() {
        return defaultLimit;
    }

    public int getExceptionLimit() {
        return exceptionLimit;
    }

    public String toString() {
        return "IP: "+IP+", taskID: "+taskID;
    }
}
