package eu.europeana.harvester.cluster.domain.messages;

import akka.actor.ActorRef;
import akka.actor.Address;

import java.io.Serializable;

/**
 * Sent by cluster master to all node masters when it notice a change in the state of a job (if it was paused or resumed)
 */
public class AddAddressToMonitor implements Serializable {

    final Address address;
    final ActorRef actorRef;

    public AddAddressToMonitor(final Address address, final ActorRef actorRef) {
        this.address = address;
        this.actorRef = actorRef;
    }

    public Address getAddress() { return address; }

    public ActorRef getActorRef() { return actorRef; }
}
