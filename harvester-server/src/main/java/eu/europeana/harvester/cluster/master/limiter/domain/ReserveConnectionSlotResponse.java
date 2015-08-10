package eu.europeana.harvester.cluster.master.limiter.domain;

import org.joda.time.DateTime;

import java.util.Random;
import java.util.UUID;

public class ReserveConnectionSlotResponse {

    public static final String generateId() {
        return UUID.randomUUID().toString();
    }

    private final String slotId;
    private final String ip;
    private final Boolean granted;

    public ReserveConnectionSlotResponse(final String ip,final Boolean granted) {
        this.slotId = generateId();
        this.ip = ip;
        this.granted = granted;
    }

    public String getSlotId() {
        return slotId;
    }

    public String getIp() {
        return ip;
    }

    public Boolean getGranted() {
        return granted;
    }
}
