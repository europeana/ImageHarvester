package eu.europeana.harvester.db;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;

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
