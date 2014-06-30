package eu.europeana.harvester.eventbus;

public interface Filter {

	public abstract boolean Apply(Event event);

}
