package eu.europeana.harvester.cluster.master.limiter.domain;

import java.io.Serializable;

public class ChangeMaxAvailableSlotsRequest implements Serializable {
    private final String ip;
    private final Integer maxAvailableSlots;

    public ChangeMaxAvailableSlotsRequest(String ip, Integer maxAvailableSlots) {
        this.ip = ip;
        this.maxAvailableSlots = maxAvailableSlots;
    }

    public String getIp() {
        return ip;
    }

    public Integer getMaxAvailableSlots() {
        return maxAvailableSlots;
    }
}
