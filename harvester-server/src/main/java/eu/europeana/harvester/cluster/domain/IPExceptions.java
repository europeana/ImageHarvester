package eu.europeana.harvester.cluster.domain;

import java.util.List;

public class IPExceptions {

    private final Integer maxConcurrentConnectionsLimit;

    private final List<String> ips;

    private final List<String> ignoredIPs;

    public IPExceptions(Integer maxConcurrentConnectionsLimit, List<String> ips, List<String> ignoredIPs) {
        this.maxConcurrentConnectionsLimit = maxConcurrentConnectionsLimit;
        this.ips = ips;
        this.ignoredIPs = ignoredIPs;
    }

    public Integer getMaxConcurrentConnectionsLimit() {
        return maxConcurrentConnectionsLimit;
    }

    public List<String> getIps() {
        return ips;
    }

    public List<String> getIgnoredIPs() {
        return ignoredIPs;
    }
}
