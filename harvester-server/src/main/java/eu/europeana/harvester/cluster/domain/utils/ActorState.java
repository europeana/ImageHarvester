package eu.europeana.harvester.cluster.domain.utils;

/**
 * These are the states of the low level downloader actors. If their state is
 * BUSY we can't send them another message.
 */
public enum ActorState {
    /**
     * When an actor is waiting for a new task.
     */
    READY,
    /**
     * When an actor has a task to do.
     */
    BUSY
}
