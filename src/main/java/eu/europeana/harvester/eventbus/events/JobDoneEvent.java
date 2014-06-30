package eu.europeana.harvester.eventbus.events;

import eu.europeana.harvester.eventbus.Event;

import java.util.Date;

public class JobDoneEvent implements Event {

    private final String id;
    private final Date date;

    public JobDoneEvent(String id) {
        this.id = id;
        this.date = new Date();
    }

    public String getId() {
        return id;
    }
}
