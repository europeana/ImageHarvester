package eu.europeana.publisher.domain;

import com.typesafe.config.Config;

public class GraphiteReporterConfig {
    private final String graphiteServer;
    private final String graphiteMasterId;
    private final int graphitePort;

    public GraphiteReporterConfig(Config config) {
        graphiteMasterId = config.getString("metrics.masterID");
        graphiteServer = config.getString("metrics.graphiteServer");
        graphitePort = config.getInt("metrics.graphitePort");
    }

    public GraphiteReporterConfig (String graphiteServer, String graphiteMasterId, int graphitePort) {
        this.graphiteServer = graphiteServer;
        this.graphiteMasterId = graphiteMasterId;
        this.graphitePort = graphitePort;
    }

    public String getGraphiteServer() {
        return graphiteServer;
    }

    public String getGraphiteMasterId() {
        return graphiteMasterId;
    }

    public int getGraphitePort() {
        return graphitePort;
    }
}
