package eu.europeana.publisher.domain;

import com.typesafe.config.Config;

public class MongoConfig {

    /**
     * MongoDB host.
     */
    private final String host;

    /**
     * MongoDB port.
     */
    private final Integer port;

    /**
     * The DB name.
     */
    private final String dBName;

    /**
     * The username of the  DB if it is secured.
     */
    private final String dBUsername;

    /**
     * The password of the DB if it is secured.
     */
    private final String dBPassword;

    public MongoConfig (final Config mongoConfig) {
        host = mongoConfig.getString("host");
        port = mongoConfig.getInt("port");
        dBName = mongoConfig.getString("dbName");
        dBUsername = mongoConfig.getString("username");
        dBPassword = mongoConfig.getString("password");
    }

    public MongoConfig (String host, Integer port, String dBName, String dBUsername, String dBPassword) {
        this.host = host;
        this.port = port;
        this.dBName = dBName;
        this.dBUsername = dBUsername;
        this.dBPassword = dBPassword;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getdBName() {
        return dBName;
    }

    public String getdBUsername() {
        return dBUsername;
    }

    public String getdBPassword() {
        return dBPassword;
    }
}
