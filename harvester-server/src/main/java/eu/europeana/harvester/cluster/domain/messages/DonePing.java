package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Message sent by slave actor and node master actor after the ping is done.
 */
public class DonePing implements Serializable {

    /**
     * Servers ip address and a reference to a MachineResourceReference
     */
    private final String ipAddress;

    /**
     * Average ping time.
     */
    private final Double avgTime;

    /**
     * Min ping time.
     */
    private final Long minTime;

    /**
     * Max ping time.
     */
    private final Long maxTime;

    /**
     * Median deviation.
     */
    private final Double medianDeviation;

    /**
     * There is a case when in the server the ICMP messages are disabled, then we can't collect data.
     */
    private final Boolean success;

    public DonePing() {
        this.ipAddress = null;
        this.avgTime = null;
        this.minTime = null;
        this.maxTime = null;
        this.medianDeviation = null;
        this.success = false;
    }

    public DonePing(final String ipAddress, final Double avgTime, final Long minTime,
                    final Long maxTime, final Double medianDeviation) {
        this.ipAddress = ipAddress;
        this.avgTime = avgTime;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.medianDeviation = medianDeviation;
        this.success = true;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public Double getAvgTime() {
        return avgTime;
    }

    public Long getMinTime() {
        return minTime;
    }

    public Long getMaxTime() {
        return maxTime;
    }

    public Double getMedianDeviation() {
        return medianDeviation;
    }

    public Boolean getSuccess() {
        return success;
    }
}
