package eu.europeana.harvester.domain;

import eu.europeana.harvester.db.swift.SwiftConfiguration;

public class MediaStorageClientConfig {

    private final MongoConfig mongoConfig;
    private final String namespaceName;
    private final SwiftConfiguration swiftConfiguration;

    public MediaStorageClientConfig (MongoConfig mongoConfig, String namespaceName) {
        this.mongoConfig = mongoConfig;
        this.namespaceName = namespaceName;
        this.swiftConfiguration = null;
    }

    public MediaStorageClientConfig (final SwiftConfiguration swiftConfiguration) {
        this.mongoConfig = null;
        this.namespaceName = null;
        this.swiftConfiguration = null;
    }

    public String getNamespaceName() {
        return namespaceName;
    }

    public MongoConfig getMongoConfig () {
        return mongoConfig;
    }

    public SwiftConfiguration getSwiftConfiguration() {
        return swiftConfiguration;
    }
}
