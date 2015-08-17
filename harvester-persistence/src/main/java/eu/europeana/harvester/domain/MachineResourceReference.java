package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

public class MachineResourceReference {

    @Id
    @Property("ipaddress")
    private final String id;

    private final Integer maxConcurrentConnectionsLimit;

    public MachineResourceReference() {
        this.id = null;
        this.maxConcurrentConnectionsLimit = null;
    }

    public MachineResourceReference(final String ip) {
        this.id = ip;
        this.maxConcurrentConnectionsLimit = null;
    }

    public MachineResourceReference(String id, Integer maxConcurrentConnectionsLimit) {
        this.id = id;
        this.maxConcurrentConnectionsLimit = maxConcurrentConnectionsLimit;
    }

    public String getId() {
        return id;
    }

    public String getIp() {
        return id;
    }

    public Integer getMaxConcurrentConnectionsLimit() {
        return maxConcurrentConnectionsLimit;
    }
}
