package eu.europeana.harvester.cluster.domain;

import org.joda.time.Duration;

/**
 * Stores various configuration properties needed by the ping master actor.
 */
public class PingMasterConfig {

    /**
     * The time interval in milliseconds between each ping.
     */
    private final Duration newPingInterval;

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

    public PingMasterConfig(Duration newPingInterval, Integer nrOfPings,
                            Duration receiveTimeoutInterval, Integer pingTimeout) {
        this.newPingInterval = newPingInterval;
        this.nrOfPings = nrOfPings;
        this.receiveTimeoutInterval = receiveTimeoutInterval;
        this.pingTimeout = pingTimeout;
    }

    public Duration getNewPingInterval() {
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
}
