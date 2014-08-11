package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Sent by the cluster master to the node master actor if it is rejoining the system
 * to ensure that it has no tasks in it's message box and it has nothing to work on at the fresh joining.
 */
public class Clean implements Serializable {
}
