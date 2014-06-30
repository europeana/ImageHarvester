package eu.europeana.harvester.client;

import com.mongodb.WriteConcern;

public class HarvesterClientConfig {

    private final WriteConcern writeConcern;

    public HarvesterClientConfig(WriteConcern writeConcern) {
        this.writeConcern = writeConcern;
    }

    public WriteConcern getWriteConcern() {
        return writeConcern;
    }
}
