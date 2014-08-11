package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.Date;
import java.util.UUID;

/**
 * Contains the result of a ping job on a specified IP address.
 */
public class MachineResourceReferenceStat {

    @Id
    @Property("id")
    private final String id;

    /**
     * Servers ip address and a reference to a MachineResourceReference
     */
    private final String ipAddress;

    /**
     * Last update time
     */
    private final Date timeStamp;

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

    public MachineResourceReferenceStat() {
        this.id = null;
        this.ipAddress = null;
        this.timeStamp = null;
        this.avgTime = null;
        this.minTime = null;
        this.maxTime = null;
        this.medianDeviation = null;
    }

    public MachineResourceReferenceStat(final String ipAddress, final Date timeStamp, final Double avgTime,
                                        final Long minTime, final Long maxTime, final Double medianDeviation) {
        this.id = UUID.randomUUID().toString();
        this.ipAddress = ipAddress;
        this.timeStamp = timeStamp;
        this.avgTime = avgTime;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.medianDeviation = medianDeviation;
    }

    public MachineResourceReferenceStat(final String id, final String ipAddress, final Date timeStamp,
                                        final Double avgTime, final Long minTime, final Long maxTime,
                                        final Double medianDeviation) {
        this.id = id;
        this.ipAddress = ipAddress;
        this.timeStamp = timeStamp;
        this.avgTime = avgTime;
        this.minTime = minTime;
        this.maxTime = maxTime;
        this.medianDeviation = medianDeviation;
    }

    public String getId() {
        return id;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public Date getTimeStamp() {
        return timeStamp;
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
}
