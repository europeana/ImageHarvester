package eu.europeana.harvester.cluster.domain;

import com.mongodb.WriteConcern;
import org.joda.time.Duration;

/**
 * Stores various configuration properties needed by the ping master actor.
 */
public class PingMasterConfig {

    /**
     * The time interval in milliseconds between each ping.
     */
    private final Integer newPingInterval;

    /**
     * How many times we send ICMP messages to the server.
     */
    private final Integer nrOfPings;

    /**
     * The time interval in milliseconds after that akka actor gives timeout error.
     */
    private final Duration receiveTimeoutInterval;

    /**
     * The interval in ms after that single ping stops.
     */
    private final Integer pingTimeout;

    /**
     * Describes the guarantee that MongoDB provides when reporting on the success of a write operation
     */
    private final WriteConcern writeConcern;

    public PingMasterConfig(final Integer newPingInterval, final Integer nrOfPings,
                            final Duration receiveTimeoutInterval, final Integer pingTimeout,
                            final WriteConcern writeConcern) {
        this.newPingInterval = newPingInterval;
        this.nrOfPings = nrOfPings;
        this.receiveTimeoutInterval = receiveTimeoutInterval;
        this.pingTimeout = pingTimeout;
        this.writeConcern = writeConcern;
    }

    public Integer getNewPingInterval() {
        return newPingInterval;
    }

    public Duration getReceiveTimeoutInterval() {
        return receiveTimeoutInterval;
    }

    public Integer getNrOfPings() {
        return nrOfPings;
    }

    public Integer getPingTimeout() {
        return pingTimeout;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
