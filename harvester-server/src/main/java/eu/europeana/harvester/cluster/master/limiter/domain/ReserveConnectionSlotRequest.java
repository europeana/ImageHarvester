package eu.europeana.harvester.cluster.master.limiter.domain;

public class ReserveConnectionSlotRequest {

    private final String ip;

    public ReserveConnectionSlotRequest(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return ip;
    }
}
