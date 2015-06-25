package eu.europeana.uimtester.domain;

import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.europeana.harvester.db.swift.SwiftConfiguration;

import java.io.File;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by salexandru on 19.06.2015.
 */
public class UIMTesterConfig {
    private final List<ServerAddress> serverAddressList;
    private final String mongoDBName;
    private final String mongoDBUserName;
    private final String mongoDBPassword;
    private final SwiftConfiguration swiftConfiguration;
    private final boolean useSwift;

    private final WriteConcern writeConcern;

    public UIMTesterConfig (final File uimTesterConfigFile) throws UnknownHostException {
        if (null == uimTesterConfigFile) {
            throw new IllegalArgumentException("No null paramters is allowed");
        }
        final Config config = ConfigFactory.parseFile(uimTesterConfigFile);

        mongoDBName = config.getString("crfHarvesterMongo.dbName");
        mongoDBUserName = config.getString("crfHarvesterMongo.username");
        mongoDBPassword = config.getString("crfHarvesterMongo.password");

        final String host = config.getString("crfHarvesterSwift.host");
        final String tenantName = config.getString("crfHarvesterSwift.tenantName");
        final String userName = config.getString("crfHarvesterSwift.userName");
        final String password = config.getString("crfHarvesterSwift.password");
        final String regionName = config.getString("crfHarvesterSwift.regionName");
        final String containerName = config.getString("crfHarvesterSwift.containerName");


        swiftConfiguration = new SwiftConfiguration(host, tenantName, userName, password, containerName, regionName);

        useSwift = config.getBoolean("useSwift");

        writeConcern = WriteConcern.valueOf(config.getString("writeConcern").toUpperCase());

        serverAddressList = new LinkedList<>();

        for (final Config hostConfig: config.getConfigList("crfHarvesterMongo.hosts")) {
            serverAddressList.add(new ServerAddress(hostConfig.getString("host"), hostConfig.getInt("port")));
        }
    }

    public UIMTesterConfig (List<ServerAddress> serverAddressList, String mongoDBName,
                            String mongoDBUserName, String mongoDBPassword, SwiftConfiguration swiftConfiguration,
                            boolean useSwift, WriteConcern writeConcern) {
        this.serverAddressList = serverAddressList;
        this.mongoDBName = mongoDBName;
        this.mongoDBUserName = mongoDBUserName;
        this.mongoDBPassword = mongoDBPassword;
        this.useSwift = useSwift;
        this.swiftConfiguration = swiftConfiguration;
        this.writeConcern = writeConcern;
    }

    public List<ServerAddress> getServerAddressList() {return serverAddressList;}


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

    public SwiftConfiguration getSwiftConfiguration () {
        return swiftConfiguration;
    }

    public boolean useSwift () {
        return useSwift;
    }
}
