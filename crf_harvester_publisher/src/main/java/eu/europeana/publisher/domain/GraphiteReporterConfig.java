package eu.europeana.publisher.domain;

import com.typesafe.config.Config;

public class GraphiteReporterConfig {
    private final String server;
    private final String masterId;
    private final int port;

    public GraphiteReporterConfig(Config config) {
        masterId = config.getString("metrics.masterID");
        server = config.getString("metrics.server");
        port = config.getInt("metrics.port");
    }

    public GraphiteReporterConfig (String server, String masterId, int port) {
        this.server = server;
        this.masterId = masterId;
        this.port = port;
    }

    public String getServer () {
        return server;
    }

    public String getMasterId () {
        return masterId;
    }

    public int getPort () {
        return port;
    }
}
