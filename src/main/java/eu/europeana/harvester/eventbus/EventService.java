package eu.europeana.harvester.eventbus;

import java.util.ArrayList;
import java.util.List;

public class EventService {

	protected List<Subscription> subscriptions;

	public EventService() {
		subscriptions = new ArrayList<Subscription>();
	}

	public void publish(Event e) {
		for(Subscription record : subscriptions)
			if(record.getEventType().isAssignableFrom(e.getClass()) &&
                    (record.getFilter() == null || record.getFilter().Apply(e))) {
				record.getSubscriber().inform(e);
            }
	}

	public void subscribe(Class eventType, Filter filter, Subscriber subscriber) {
		if(Event.class.isAssignableFrom(eventType)) {
			Subscription _record = new Subscription(eventType, filter, subscriber);
			if(!subscriptions.contains(_record)) {
				subscriptions.add(_record);
            }
		}
	}

	public void unsubscribe(Class eventType, Filter filter, Subscriber subscriber) {
		if(Event.class.isAssignableFrom(eventType)) {
			subscriptions.remove(new Subscription(eventType, filter, subscriber));
        }
	}
}
