package eu.europeana.harvester.db;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import java.net.UnknownHostException;
import java.util.List;

/**
 * Wrapper around the morphia datastore that simplifies the creation of the stores.
 */
public class MorphiaDataStore {

    private final MongoClient mongo;
    private final Morphia morphia;
    private final Datastore datastore;

    public MorphiaDataStore(String host, int port, String dbName) throws UnknownHostException {
        mongo = new MongoClient(host, port);
        morphia = new Morphia();
        datastore = morphia.createDatastore(mongo, dbName);
    }

    public MorphiaDataStore (String host, int port, String dbName, String username, String password) throws UnknownHostException {
        mongo = new MongoClient(host, port);

        if (!mongo.getDB("admin").authenticate(username, password.toCharArray())) {
            throw new RuntimeException("Couldn't login to mongo");
        }

        morphia = new Morphia();
        datastore = morphia.createDatastore(mongo, dbName);
    }

    public MorphiaDataStore(final List<ServerAddress> serverAddressList, String dbName) {
       mongo = new MongoClient(serverAddressList);
       morphia = new Morphia();
       datastore = morphia.createDatastore(mongo, dbName);
    }

    public MongoClient getMongo() {
        return mongo;
    }

    public Morphia getMorphia() {
        return morphia;
    }

    public Datastore getDatastore() {
        return datastore;
    }
}
