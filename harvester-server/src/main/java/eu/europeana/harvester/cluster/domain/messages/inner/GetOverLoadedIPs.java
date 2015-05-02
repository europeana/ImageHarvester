package eu.europeana.harvester.cluster.domain.messages.inner;

import java.io.Serializable;

public class GetOverLoadedIPs implements Serializable {

    private final int threshold;

    public GetOverLoadedIPs(int threshold) {
        this.threshold = threshold;
    }

    public int getThreshold() {
        return threshold;
    }
}
