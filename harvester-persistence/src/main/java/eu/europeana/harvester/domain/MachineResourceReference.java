package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

public class MachineResourceReference {

    @Id
    @Property("ipaddress")
    private final String id;

    public MachineResourceReference() {
        this.id = null;
    }

    public MachineResourceReference(final String ip) {
        this.id = ip;
    }

    public String getId() {
        return id;
    }

    public String getIp() {
        return id;
    }

}
