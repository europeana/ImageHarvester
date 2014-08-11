package eu.europeana.harvester.cluster.domain.messages;

import java.io.Serializable;

/**
 * Message sent by the ping master actor to slaves when it want to ping an ip.
 */
public class StartPing implements Serializable {

    /**
     * Servers ip address.
     */
    private final String ip;

    /**
     * How many times we send ICMP messages to the server.
     */
    private final Integer nrOfPings;

    /**
     * The interval in ms after that single ping stops.
     */
    private final Integer pingTimeout;

    public StartPing(final String ip, final Integer nrOfPings, final Integer pingTimeout) {
        this.ip = ip;
        this.nrOfPings = nrOfPings;
        this.pingTimeout = pingTimeout;
    }

    public String getIp() {
        return ip;
    }

    public Integer getNrOfPings() {
        return nrOfPings;
    }

    public Integer getPingTimeout() {
        return pingTimeout;
    }
}
