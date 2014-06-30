package eu.europeana.harvester.eventbus;

class Subscription {

    private Class eventType;
    private Filter filter;
    private Subscriber subscriber;

    public Subscription(Class anEventType, Filter aFilter, Subscriber aSubscriber) {
        eventType = anEventType;
        filter = aFilter;
        subscriber = aSubscriber;
    }

    public Class getEventType() {
        return eventType;
    }


    public Filter getFilter() {
        return filter;
    }

    public Subscriber getSubscriber() {
        return subscriber;
    }
}
