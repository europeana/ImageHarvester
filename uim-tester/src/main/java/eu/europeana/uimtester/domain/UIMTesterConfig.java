package eu.europeana.uimtester.domain;

import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.File;

/**
 * Created by salexandru on 19.06.2015.
 */
public class UIMTesterConfig {
    private final String mongoHost;
    private final Integer mongoPort;
    private final String mongoDBName;
    private final String mongoDBUserName;
    private final String mongoDBPassword;

    private final WriteConcern writeConcern;

    public UIMTesterConfig (final File uimTesterConfigFile) {
        if (null == uimTesterConfigFile) {
            throw new IllegalArgumentException("No null paramters is allowed");
        }
        final Config config = ConfigFactory.parseFile(uimTesterConfigFile);

        mongoHost = config.getString("mongo.host");
        mongoPort = config.getInt("mongo.port");
        mongoDBName = config.getString("mongo.dbName");
        mongoDBUserName = config.getString("mongo.username");
        mongoDBPassword = config.getString("mongo.password");

        writeConcern = WriteConcern.valueOf(config.getString("writeConcern").toUpperCase());

    }

    public UIMTesterConfig (String mongoHost, Integer mongoPort, String mongoDBName,
                            String mongoDBUserName, String mongoDBPassword, WriteConcern writeConcern) {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDBName = mongoDBName;
        this.mongoDBUserName = mongoDBUserName;
        this.mongoDBPassword = mongoDBPassword;
        this.writeConcern = writeConcern;
    }

    public String getMongoHost () {
        return mongoHost;
    }

    public Integer getMongoPort () {
        return mongoPort;
    }

    public String getMongoDBName () {
        return mongoDBName;
    }

    public String getMongoDBUserName () {
        return mongoDBUserName;
    }

    public String getMongoDBPassword () {
        return mongoDBPassword;
    }

    public WriteConcern getWriteConcern () {
        return writeConcern;
    }
}
