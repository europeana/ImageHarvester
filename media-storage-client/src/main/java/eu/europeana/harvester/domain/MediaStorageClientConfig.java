package eu.europeana.harvester.domain;

public class MediaStorageClientConfig {

    private final MongoConfig mongoConfig;
    private final String namespaceName;

    public MediaStorageClientConfig (MongoConfig mongoConfig, String namespaceName) {
        this.mongoConfig = mongoConfig;
        this.namespaceName = namespaceName;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public MongoConfig getMongoConfig () {
        return mongoConfig;
    }
}
