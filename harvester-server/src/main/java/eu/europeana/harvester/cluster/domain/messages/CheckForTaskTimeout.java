package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Periodically sent by cluster master to itself to check if there are any tasks with no initial feedback from any slave.
 * This means that no one received it, so we the master has to resend them.
 */
public class CheckForTaskTimeout implements Serializable {
}
