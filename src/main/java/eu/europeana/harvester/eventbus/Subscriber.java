package eu.europeana.harvester.eventbus;

public interface Subscriber {

	public void inform(Event event);

}
