package eu.europeana.harvester.cluster.master.limiter;

import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotRequest;
import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotResponse;
import eu.europeana.harvester.cluster.master.limiter.domain.ReturnConnectionSlotRequest;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

public class IpLimiterAccountant {

    private final Integer defaultLimitsPerIp;
    private final Map<String,Integer> specificLimitsPerIp;
    private final Map<String /* IP */, IpConnectionSlots> occupiedConnectionSlotsPerIp;

    public IpLimiterAccountant(Integer defaultLimitsPerIp,Map<String,Integer> specificLimitsPerIp) {
        this.defaultLimitsPerIp = defaultLimitsPerIp;
        this.specificLimitsPerIp = specificLimitsPerIp;
        this.occupiedConnectionSlotsPerIp = new HashMap<>();
    }

    private final Integer computeLimitPerIp(final String ip) {
        if (specificLimitsPerIp.containsKey(ip)) return specificLimitsPerIp.get(ip);
        else return defaultLimitsPerIp;
    }

    private void occupiedConnectionSlotsPerIpFull(final String ip) {
        if (!occupiedConnectionSlotsPerIp.containsKey(ip)) occupiedConnectionSlotsPerIp.put(ip,new IpConnectionSlots(computeLimitPerIp(ip),ip));
    }

    public final ReserveConnectionSlotResponse reserveConnectionSlotRequest(final ReserveConnectionSlotRequest reserveConnectionSlotRequest) {
        occupiedConnectionSlotsPerIpFull(reserveConnectionSlotRequest.getIp());
        return occupiedConnectionSlotsPerIp.get(reserveConnectionSlotRequest.getIp()).requestConnectionSlotReservation();
    }

    public final boolean returnConnectionSlotRequest(final ReturnConnectionSlotRequest returnConnectionSlotRequest) {
        occupiedConnectionSlotsPerIpFull(returnConnectionSlotRequest.getIp());
        return occupiedConnectionSlotsPerIp.get(returnConnectionSlotRequest.getIp()).returnConnectionSlotReservation(returnConnectionSlotRequest.getSlotId());
    }

    public final int reclaimOccupiedSlotsOlderThan(final DateTime limit) {
        int reclaimedSlots = 0;
        for (final String ip : occupiedConnectionSlotsPerIp.keySet()) {
            reclaimedSlots = reclaimedSlots + occupiedConnectionSlotsPerIp.get(ip).reclaimOccupiedSlotsOlderThan(limit);
        }
        return reclaimedSlots;
    }

    }
