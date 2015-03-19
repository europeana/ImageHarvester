package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;
import java.util.HashMap;

public class CheckIPsWithJobs implements Serializable {

    private final HashMap<String, Boolean> ipsWithJobs;

    public CheckIPsWithJobs(HashMap<String, Boolean> ipsWithJobs) {
        this.ipsWithJobs = ipsWithJobs;
    }

    public HashMap<String, Boolean> getIpsWithJobs() {
        return ipsWithJobs;
    }
}
