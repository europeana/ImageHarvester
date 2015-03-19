package eu.europeana.harvester.domain;

public class MediaStorageClientConfig {

    /**
     * MongoDB host.
     */
    private final String host;

    /**
     * MongoDB port.
     */
    private final Integer port;

    /**
     * The username of the given MongoDB if it is encrypted.
     */
    private final String username;

    /**
     * The password of the given MongoDB if it is encrypted.
     */
    private final String password;

    /**
     * The DB name where the thumbnails will be stored.
     */
    private final String dbName;

    private final String namespaceName;

    public MediaStorageClientConfig(String host, Integer port, String username, String password, String dbName, String namespaceName) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.dbName = dbName;
        this.namespaceName = namespaceName;
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

    public String getNamespaceName() {
        return namespaceName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
