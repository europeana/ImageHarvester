package eu.europeana.harvester.cluster.master.limiter.domain;

import org.joda.time.Duration;

import java.util.Map;

public class IPLimiterConfig {

    private final Integer defaultLimitsPerIp;
    private final Map<String,Integer> specificLimitsPerIp;

    private final Duration maxSlotUsageLife;

    public IPLimiterConfig(Integer defaultLimitsPerIp, Map<String, Integer> specificLimitsPerIp, Duration maxSlotUsageLife) {
        this.defaultLimitsPerIp = defaultLimitsPerIp;
        this.specificLimitsPerIp = specificLimitsPerIp;
        this.maxSlotUsageLife = maxSlotUsageLife;
    }

    public Integer getDefaultLimitsPerIp() {
        return defaultLimitsPerIp;
    }

    public Duration getMaxSlotUsageLife() {
        return maxSlotUsageLife;
    }

    public Map<String, Integer> getSpecificLimitsPerIp() {
        return specificLimitsPerIp;
    }
}
