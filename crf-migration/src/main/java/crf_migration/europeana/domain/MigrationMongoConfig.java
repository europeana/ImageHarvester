package crf_migration.europeana.domain;

public class MigrationMongoConfig {

    /**
     * MongoDB host.
     */
    private final String host;

    /**
     * MongoDB port.
     */
    private final Integer port;

    /**
     * The DB name where the thumbnails will be stored.
     */
    private final String dbName;

    public MigrationMongoConfig(String host, Integer port, String dbName) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }
}
