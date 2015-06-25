package eu.europeana.crfmigration.domain;

import com.mongodb.ServerAddress;

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
