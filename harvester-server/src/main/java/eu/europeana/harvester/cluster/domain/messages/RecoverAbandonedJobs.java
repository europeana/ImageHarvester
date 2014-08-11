package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Sent to cluster master at startup to notice it to check the db
 * if there are any abandoned jobs and if so then to reload them.
 */
public class RecoverAbandonedJobs implements Serializable {
}
