package eu.europeana.harvester.domain;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

public class MongoConfig {

    private final List<ServerAddress> mongoServerAddressList;


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


    public MongoConfig(List<ServerAddress> mongoServerAddressList, String dBName, String dBUsername, String dBPassword) {
        this.mongoServerAddressList = mongoServerAddressList;
        this.dBName = dBName;
        this.dBUsername = dBUsername;
        this.dBPassword = dBPassword;
    }

    public static MongoConfig valueOf(final Config config) throws UnknownHostException {
        final List<ServerAddress> serverAddresses = new LinkedList<>();

        for (final Config hostConfig: config.getConfigList("hosts")) {
            serverAddresses.add(new ServerAddress(hostConfig.getString("host"), hostConfig.getInt("port")));
        }

        return new MongoConfig(serverAddresses,
                               config.getString("dBName"),
                               config.getString("dBUsername"),
                               config.getString("dBPassword")
                             );
    }


    public Mongo connectToMongo() {
       final Mongo mongo = new Mongo(mongoServerAddressList);

        if (StringUtils.isNotEmpty(dBUsername)) {
           if (!mongo.getDB("admin").authenticate(dBUsername, dBPassword.toCharArray())) {
               return null;
           }
        }

        return mongo;
    }

    public DB connectToDB () {
        final Mongo mongo = connectToMongo();
       return null != mongo ? mongo.getDB(dBName) : null;
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

    public List<ServerAddress> getMongoServerAddressList () {
        return mongoServerAddressList;
    }
}
