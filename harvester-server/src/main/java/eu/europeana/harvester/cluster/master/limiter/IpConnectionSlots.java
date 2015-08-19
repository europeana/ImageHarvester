package eu.europeana.harvester.cluster.master.limiter;

import eu.europeana.harvester.cluster.master.limiter.domain.ReserveConnectionSlotResponse;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class IpConnectionSlots {

    private Integer maxAvailableSlots;
    private final String ip;
    private final HashMap<String /* Slot token */, DateTime /* The time when it was granted */> slots;

    public IpConnectionSlots(Integer maxAvailableSlots, String ip) {
        this.maxAvailableSlots = maxAvailableSlots;
        this.ip = ip;
        this.slots = new HashMap<String, DateTime>(maxAvailableSlots);
    }

    public final ReserveConnectionSlotResponse requestConnectionSlotReservation(final String taskId) {
        ReserveConnectionSlotResponse response = null;
        if (slots.keySet().size() < maxAvailableSlots) {
            response = new ReserveConnectionSlotResponse(ip,taskId, true);
            slots.put(response.getSlotId(), DateTime.now());
        } else {
            response = new ReserveConnectionSlotResponse(ip,taskId , false);
        }
        return response;
    }

    public final boolean returnConnectionSlotReservation(final String slotId) {
        if (slots.containsKey(slotId)) {
            slots.remove(slotId);
            return true;
        } else {
            return false;
        }
    }

    public final Integer reclaimOccupiedSlotsOlderThan(final DateTime limit) {
        final List<String> slotIdsToBeReclaimed = new ArrayList<>();
        for (final String slotId : slots.keySet()) {
            if (slots.get(slotId).isBefore(limit)) slotIdsToBeReclaimed.add(slotId);
        }

        for (final String slotId : slotIdsToBeReclaimed) {
            slots.remove(slotId);
        }
        return slotIdsToBeReclaimed.size();
    }

    public final int getNumberOfAvailableSlots() {
        return maxAvailableSlots-slots.keySet().size();
    }

    public void setMaxAvailableSlots(Integer maxAvailableSlots) {
        this.maxAvailableSlots = maxAvailableSlots;
    }
}
