package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;
import java.util.ArrayList;

public class CleanIPs implements Serializable {

    private final ArrayList<String> IPs;

    public CleanIPs(ArrayList<String> IPs) {
        this.IPs = IPs;
    }

    public ArrayList<String> getIPs() {
        return IPs;
    }
}
