package eu.europeana.harvester.eventbus.subscribers;

import eu.europeana.harvester.eventbus.Event;
import eu.europeana.harvester.eventbus.EventService;
import eu.europeana.harvester.eventbus.Subscriber;
import eu.europeana.harvester.eventbus.events.JobDoneEvent;

public class JobDoneSubscriber implements Subscriber {

    private final EventService eventService;

    public JobDoneSubscriber(EventService eventService) {
        this.eventService = eventService;
        eventService.subscribe(JobDoneEvent.class, null, this);
    }

    @Override
    public void inform(Event event) {
        String id = ((JobDoneEvent)event).getId();

        System.out.println("Finished. Job with id: " + id);
    }

}
