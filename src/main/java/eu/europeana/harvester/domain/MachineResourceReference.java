package eu.europeana.harvester.domain;

import com.google.code.morphia.annotations.Id;
import com.google.code.morphia.annotations.Property;

import java.util.Date;
import java.util.List;

/**
 * Cluster wide processing limits per collection.
 */
public class MachineResourceReference {

    @Id
    @Property("ipaddress")
    private final String id;

    /**
     * The date when the server was last time pinged
     */
    private final Date lastPingCheck;

    /**
     * A list of reference owners. One reference owner has 3 attributes: provider, collection and record id
     */
    private final List<ReferenceOwner> referenceOwnerList;

    /**
     * The bandwidth limit usage for read (ie. receiving). Measured in bytes. 0 means no limit.
     */
    private final Long bandwidthLimitReadInBytesPerSec;

    /**
     * The maximum number of concurrent connections allowed per collection.
     */
    private final Long maxConcurrentConnectionsLimit;

    public MachineResourceReference() {
        this.id = null;
        this.lastPingCheck = null;
        this.referenceOwnerList = null;
        this.bandwidthLimitReadInBytesPerSec = null;
        this.maxConcurrentConnectionsLimit = null;
    }

    public MachineResourceReference(String id, Date lastPingCheck, List<ReferenceOwner> referenceOwnerList, Long bandwidthLimitReadInBytesPerSec,
                                    Long maxConcurrentConnectionsLimit) {
        this.id = id;
        this.lastPingCheck = lastPingCheck;
        this.referenceOwnerList = referenceOwnerList;
        this.bandwidthLimitReadInBytesPerSec = bandwidthLimitReadInBytesPerSec;
        this.maxConcurrentConnectionsLimit = maxConcurrentConnectionsLimit;
    }

    public String getId() {
        return id;
    }

    public Long getBandwidthLimitReadInBytesPerSec() {
        return bandwidthLimitReadInBytesPerSec;
    }

    public Long getMaxConcurrentConnectionsLimit() {
        return maxConcurrentConnectionsLimit;
    }

    public Date getLastPingCheck() {
        return lastPingCheck;
    }

    public List<ReferenceOwner> getReferenceOwnerList() {
        return referenceOwnerList;
    }

    public MachineResourceReference withLastPingCheck(Date lastPingCheck) {
        return new MachineResourceReference(this.id, lastPingCheck, this.referenceOwnerList,
                this.bandwidthLimitReadInBytesPerSec, this.maxConcurrentConnectionsLimit);
    }

    public MachineResourceReference withReferenceOwnerList(List<ReferenceOwner> referenceOwnerList) {
        return new MachineResourceReference(this.id, this.lastPingCheck, referenceOwnerList,
                this.bandwidthLimitReadInBytesPerSec, this.maxConcurrentConnectionsLimit);
    }
}
