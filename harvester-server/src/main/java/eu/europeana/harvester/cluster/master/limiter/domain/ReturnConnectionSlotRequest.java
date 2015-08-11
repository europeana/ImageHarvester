package eu.europeana.harvester.cluster.master.limiter.domain;

import java.io.Serializable;

public class ReturnConnectionSlotRequest implements Serializable{

    private final String slotId;
    private final String ip;

    public ReturnConnectionSlotRequest(String slotId, String ip) {
        this.slotId = slotId;
        this.ip = ip;
    }

    public String getSlotId() {
        return slotId;
    }

    public String getIp() {
        return ip;
    }
}
